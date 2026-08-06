#!/usr/bin/env bash
#
# One-shot driver for the whole dataset pipeline (README steps 1-6).
#
# Each step is resumable: the fetcher skips work already in the DB, and the
# classifier passes are safe to re-run. If a step dies, fix it and re-run with
# --from <step>.
#
# Usage:
#   scripts/build-dataset.sh                          # steps 1-5, no upload
#   scripts/build-dataset.sh --from classify          # resume at step 3
#   scripts/build-dataset.sh --only export
#   scripts/build-dataset.sh --upload                 # include step 6 (Kaggle)
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- defaults (override via flags or env) ---
QUERY="${QUERY:-filename:SKILL.md}"
MAIN_DB="${MAIN_DB:-data/skills.db}"
VALIDATED_DB="${VALIDATED_DB:-data/validated.db}"
CONTENT_DIR="${CONTENT_DIR:-data/content}"
BUILD_DIR="${BUILD_DIR:-build}"
LABELED_CSV="${LABELED_CSV:-training/labeled.csv}"
KAGGLE_USERNAME="${KAGGLE_USERNAME:-}"
FETCHER="git+https://github.com/thekevinscott/github-data-file-fetcher"

# uv's 30s default times out extracting the fetcher's larger wheels
# (pygithub -> pynacl -> cffi). Raise it unless the caller set one.
export UV_HTTP_TIMEOUT="${UV_HTTP_TIMEOUT:-120}"

STEPS=(paths content classify metadata export upload)
FROM=""
ONLY=""
DO_UPLOAD=0
DRY_RUN=0
ASSUME_YES=0
RESCAN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Steps (in order): ${STEPS[*]}

Options:
  --from STEP           Start at STEP (skip earlier steps)
  --only STEP           Run just STEP
  -y, --yes             Don't prompt before each step
  --rescan              Re-scan GitHub for new file paths (step 1). Bypasses
                        the API response cache and the "scan already completed"
                        short circuit. Does NOT touch fetched content.
  --upload              Include the Kaggle upload step (off by default)
  --query Q             GitHub search query        (default: $QUERY)
  --main-db PATH        Fetcher DB                 (default: $MAIN_DB)
  --validated-db PATH   Classification DB          (default: $VALIDATED_DB)
  --content-dir PATH    Fetched file content       (default: $CONTENT_DIR)
  --build-dir PATH      Parquet output             (default: $BUILD_DIR)
  --labeled-csv PATH    Classifier training data   (default: $LABELED_CSV)
  --kaggle-username U   Kaggle username for dataset metadata
  --dry-run             Print the commands without running them
  -h, --help            This message

Notes:
  * Steps 1-2 hit the GitHub API for hours and need GITHUB_TOKEN in .env.
  * Training-data generation (LLM) is deliberately NOT part of this script --
    it costs money/quota and is offline work. See README "Training".
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)            FROM="$2"; shift 2 ;;
    --only)            ONLY="$2"; shift 2 ;;
    -y|--yes)          ASSUME_YES=1; shift ;;
    --rescan)          RESCAN=1; shift ;;
    --upload)          DO_UPLOAD=1; shift ;;
    --query)           QUERY="$2"; shift 2 ;;
    --main-db)         MAIN_DB="$2"; shift 2 ;;
    --validated-db)    VALIDATED_DB="$2"; shift 2 ;;
    --content-dir)     CONTENT_DIR="$2"; shift 2 ;;
    --build-dir)       BUILD_DIR="$2"; shift 2 ;;
    --labeled-csv)     LABELED_CSV="$2"; shift 2 ;;
    --kaggle-username) KAGGLE_USERNAME="$2"; shift 2 ;;
    --dry-run)         DRY_RUN=1; shift ;;
    -h|--help)         usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

valid_step() {
  local s
  for s in "${STEPS[@]}"; do [[ "$s" == "$1" ]] && return 0; done
  return 1
}

for s in "$FROM" "$ONLY"; do
  [[ -n "$s" ]] && ! valid_step "$s" && { echo "unknown step: $s" >&2; exit 2; }
done
[[ -n "$FROM" && -n "$ONLY" ]] && { echo "--from and --only are mutually exclusive" >&2; exit 2; }

# upload is opt-in; --only upload / --from upload implies it
[[ "$ONLY" == "upload" || "$FROM" == "upload" ]] && DO_UPLOAD=1

should_run() {
  local step="$1"
  if [[ -n "$ONLY" ]]; then [[ "$step" == "$ONLY" ]]; return; fi
  if [[ -n "$FROM" ]]; then
    local seen=0 s
    for s in "${STEPS[@]}"; do
      [[ "$s" == "$FROM" ]] && seen=1
      [[ "$s" == "$step" ]] && return $(( seen ? 0 : 1 ))
    done
  fi
  return 0
}

run() {
  echo "+ $*"
  (( DRY_RUN )) && return 0
  "$@"
}

banner() { printf '\n=== %s ===\n' "$1"; }

# Gate before each step. Steps are long and some are expensive, so the default
# is to stop and let you look at the state first.
confirm() {
  local step="$1" desc="$2"
  (( ASSUME_YES || DRY_RUN )) && return 0
  # -t 0 isn't enough: /dev/tty exists as a node even with no controlling
  # terminal, so test by actually opening it.
  if ! (exec 3</dev/tty) 2>/dev/null; then
    echo "no terminal to prompt on -- re-run with --yes to proceed unattended" >&2
    exit 1
  fi
  exec 3</dev/tty
  local reply=""
  printf '\n--- next: %s ---\n%s\n' "$step" "$desc"
  read -r -u 3 -p "run it? [y/N/q] " reply || true
  exec 3<&-
  case "$reply" in
    y|Y|yes) return 0 ;;
    q|Q|quit) echo "stopping."; exit 0 ;;
    *) echo "skipping $step."; return 1 ;;
  esac
}

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }
}

need uv
need uvx

# GITHUB_TOKEN is only needed for the network steps.
if should_run paths || should_run content || should_run metadata; then
  if [[ -z "${GITHUB_TOKEN:-}" ]] && ! grep -qs '^GITHUB_TOKEN=' .env; then
    echo "GITHUB_TOKEN not set and not found in .env -- fetching will fail" >&2
    exit 1
  fi

  # Warm the fetcher's uvx env up front. Otherwise a dependency download
  # failure surfaces at step 4, hours into the run.
  if (( ! DRY_RUN )); then
    echo "+ warming uvx env for the fetcher"
    for attempt in 1 2 3; do
      uvx --from "$FETCHER" github-fetch --help >/dev/null 2>&1 && break
      if (( attempt == 3 )); then
        echo "failed to install the fetcher after 3 attempts" >&2
        exit 1
      fi
      echo "  install failed (attempt $attempt/3), retrying..." >&2
    done
  fi
fi

if (( DO_UPLOAD )) && should_run upload; then
  need kaggle
  [[ -z "$KAGGLE_USERNAME" ]] && { echo "--kaggle-username is required to upload" >&2; exit 1; }
fi

mkdir -p "$(dirname "$MAIN_DB")" "$CONTENT_DIR" "$BUILD_DIR"

# --- 1. file paths ---
if should_run paths && confirm "1/6 fetch file paths" \
  "Scan GitHub for '$QUERY' -> $MAIN_DB.$(
    (( RESCAN )) && printf '\n  --rescan: bypassing the API cache, this re-walks the whole size range (hours).' \
                 || printf '\n  Skips instantly if the scan already completed. Pass --rescan to look for new files.')"
then
  banner "1/6 fetch file paths"
  paths_args=(fetch-file-paths "$QUERY" --db "$MAIN_DB")
  (( RESCAN )) && paths_args+=(--skip-cache)
  run uvx --from "$FETCHER" github-fetch "${paths_args[@]}"
fi

# --- 2. file content ---
if should_run content && confirm "2/6 fetch file content" \
  "Download content for files in $MAIN_DB that don't have it yet -> $CONTENT_DIR.
  Already-fetched content is never re-downloaded."
then
  banner "2/6 fetch file content"
  run uvx --from "$FETCHER" github-fetch fetch-file-content \
    --db "$MAIN_DB" --content-dir "$CONTENT_DIR" --graphql
fi

# --- 3. classify (passes 1-3, no LLM) ---
if should_run classify && confirm "3/6 classify (passes 1-3)" \
  "Frontmatter -> heuristics -> SVM over $CONTENT_DIR -> $VALIDATED_DB.
  Local and free, but slow (~15min+ for pass 3). Safe to interrupt and re-run."
then
  banner "3/6 classify (passes 1-3)"
  run uv run skills-dataset filter-valid-skills \
    --main-db "$MAIN_DB" \
    --output-db "$VALIDATED_DB" \
    --content-dir "$CONTENT_DIR" \
    --labeled-csv "$LABELED_CSV"
fi

# --- 4. repo metadata + file history for the survivors ---
if should_run metadata && confirm "4/6 fetch repo metadata and history" \
  "Rebuild the 'files' table in $VALIDATED_DB from classifier survivors, then
  fetch repo metadata and commit history for them. Hits the GitHub API."
then
  banner "4/6 fetch repo metadata and history"
  run uv run skills-dataset prepare-for-fetcher --output-db "$VALIDATED_DB"
  run uvx --from "$FETCHER" github-fetch fetch-repo-metadata --db "$VALIDATED_DB"
  run uvx --from "$FETCHER" github-fetch fetch-file-history --db "$VALIDATED_DB"
fi

# --- 5. export parquet ---
if should_run export && confirm "5/6 export to Parquet" \
  "Write files/repos/history Parquet to $BUILD_DIR."
then
  banner "5/6 export to Parquet"
  export_args=(
    --main-db "$MAIN_DB"
    --validation-db "$VALIDATED_DB"
    --output-dir "$BUILD_DIR"
  )
  [[ -n "$KAGGLE_USERNAME" ]] && export_args+=(--kaggle-username "$KAGGLE_USERNAME")
  run uv run skills-dataset export "${export_args[@]}"
fi

# --- 6. upload ---
if should_run upload; then
  if (( DO_UPLOAD )); then
    if confirm "6/6 upload to Kaggle" \
      "PUBLISH $BUILD_DIR to Kaggle as $KAGGLE_USERNAME. This is public and hard to undo."
    then
      banner "6/6 upload to Kaggle"
      run env -C "$BUILD_DIR" kaggle datasets create -p . --dir-mode tar
    fi
  else
    banner "6/6 upload to Kaggle -- SKIPPED (pass --upload)"
    echo "  cd $BUILD_DIR && kaggle datasets create -p . --dir-mode tar"
  fi
fi

banner "done"
