# GitHub Skills Dataset

Build a validated dataset of Claude Code SKILL.md files from GitHub for Kaggle.

## Prerequisites

- Python >= 3.14
- A GitHub token in `.env` (`GITHUB_TOKEN=...`) for data fetching
- For training data generation only: Anthropic API key or Claude Code subscription

## Installation

```bash
uv sync
```

## Pipeline overview

Three passes classify ~1M SKILL.md files. No LLM required for classification.

```
github-data-file-fetcher              skills-dataset
========================              ==============

files.db (1.2M URLs)  ---------->  filter-pass-1 (frontmatter)
content/ (fetched files)  ------>  filter-pass-2 (heuristics)
                                   filter-pass-3 (SVM classifier)
                                        |
                                        v
                                   validated.db
                                        |
                              +---------+---------+
                              |                   |
                        export (Parquet)    generate-training-data
                              |                   |
                              v                   v
                        build/*.parquet    training/labeled.csv
                        (Kaggle dataset)   (improves classifier)
```

## Quick start

`scripts/build-dataset.sh` runs steps 1-5 end to end with the default paths below:

```bash
scripts/build-dataset.sh --kaggle-username yourname
```

Every step is resumable -- the fetcher skips what's already in the DB, and the
classifier passes are safe to re-run. If something dies, resume at that step:

```bash
scripts/build-dataset.sh --from classify     # steps: paths content classify metadata export upload
scripts/build-dataset.sh --only export
scripts/build-dataset.sh --dry-run           # print commands without running them
```

The Kaggle upload (step 6) is opt-in via `--upload`. Training data generation is
deliberately excluded -- it costs LLM budget and is offline work (see [Training](#training)).

The individual steps are documented below.

## 1. Fetch file paths

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-paths "filename:SKILL.md" --db data/skills.db
```

## 2. Fetch file content

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-content --db data/skills.db --content-dir data/content --graphql
```

The `--graphql` flag batches 50 files per query (~50x faster than REST).

## 3. Classify skills

Three passes: frontmatter check, heuristic rejection, SVM classifier. See [docs/classification.md](docs/classification.md) for details.

```bash
# Individual passes
uv run skills-dataset filter-pass-1 \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content

uv run skills-dataset filter-pass-2 \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content

uv run skills-dataset filter-pass-3 \
  --output-db data/validated.db \
  --content-dir data/content \
  --labeled-csv training/labeled.csv

# Or all 3 at once
uv run skills-dataset filter-valid-skills \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content
```

## 4. Fetch metadata and history

Prepare a `files` table in validated.db for the fetcher, then run:

```bash
uv run skills-dataset prepare-for-fetcher --output-db data/validated.db

uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-repo-metadata --db data/validated.db

uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-history --db data/validated.db
```

## 5. Export to Parquet

```bash
uv run skills-dataset export \
  --main-db data/skills.db \
  --validation-db data/validated.db \
  --output-dir build/ \
  --kaggle-username yourname
```

## 6. Upload to Kaggle

```bash
cd build && kaggle datasets create -p . --dir-mode tar
```

## Utilities

### Content hash

Compute the sha256 hash of a file (matches `sha256sum`):

```bash
uv run skills-dataset hash SKILL.md
uv run skills-dataset hash --text "---\nname: test\n---"
```

## Database schema

### `validation_results`

| Column | Type | Description |
|--------|------|-------------|
| `url` | TEXT PK | GitHub file URL |
| `has_frontmatter` | BOOLEAN | Pass 1 result |
| `checked_at` | TIMESTAMP | When frontmatter was checked |
| `heuristic_reject` | BOOLEAN | Pass 2: null=unchecked, 0=not rejected, 1=rejected |
| `heuristic_reason` | TEXT | Why heuristic rejected it |
| `classifier_is_skill` | BOOLEAN | Pass 3: classifier prediction |
| `classifier_confidence` | REAL | Pass 3: internal confidence score (stored, not exported) |

### `training/labeled.csv`

Source of truth for classifier training. Checked into git. Format:

```
# backend=claude-agent-sdk, model=claude-haiku-4-5-20251001
content_hash,is_skill
a1b2c3...,true
d4e5f6...,false
```

Content hash is `sha256(raw_file_bytes)`. Deduped by content.

## Classifier performance

With ~33K unique labeled content hashes (~684 rejects):

- **10-fold CV macro-F1: 0.937** (SVM-rbf C=10)
- **Holdout accuracy: ~90%** (balanced val set)

Performance improves with more labeled rejects. Run `generate-training-data` to produce these.

## Training

The classifier improves over time by labeling new files with an LLM. This is separate from the main pipeline -- run it when you have LLM budget.

```bash
uv run skills-dataset generate-training-data \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content \
  --backend claude-agent-sdk \
  --concurrency 8 \
  --limit 5000
```

This reads `training/labeled.csv` to skip already-labeled content, sends unlabeled files to the LLM, and appends results to the CSV. After labeling, retrain the classifier:

```bash
uv run skills-dataset filter-pass-3 \
  --output-db data/validated.db \
  --content-dir data/content
```

LLM backends:
- `--backend anthropic`: Anthropic API (per-token billing)
- `--backend openai --base-url http://host:11434/v1 --model gemma2:27b`: Ollama/local
- `--backend claude-agent-sdk`: Claude Code subscription (no API key needed)

## Development

```bash
uv sync --extra dev
uv run pytest tests/              # all 45 tests
uv run pytest tests/integration/  # unit + integration
uv run pytest tests/e2e/          # end-to-end pipeline
```
