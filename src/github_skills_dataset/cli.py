"""CLI for GitHub Skills Dataset.

Pipeline:
  filter-pass-1        Frontmatter check (free, instant)
  filter-pass-2        Heuristic reject rules (free, instant)
  filter-pass-3        SVM classifier with confidence scores (free, ~15min)
  filter-valid-skills  Run all 3 passes in sequence

Training data:
  generate-training-data  Send low-confidence URLs to LLM for labeling

Export:
  export               Export validated skills to Parquet for Kaggle
"""

import asyncio
import types

import click
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@click.group()
@click.version_option()
def cli():
    """GitHub Skills Dataset - Build SKILL.md dataset for Kaggle.

    The main pipeline (filter-pass-1 through filter-pass-3) classifies ~1M
    SKILL.md files using heuristics and an SVM classifier. No LLM required.

    The generate-training-data command sends uncertain files to an LLM to
    produce labeled examples that improve the classifier over time.
    """
    pass


# --- Shared options ---

_filter_common_options = [
    click.option("--main-db", type=click.Path(path_type=Path), required=True,
                 help="Source database from github-data-file-fetcher (contains 'files' table)"),
    click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
                 help="Output database for classification results (default: validated.db)"),
    click.option("--content-dir", type=click.Path(path_type=Path), default=Path("content"),
                 help="Directory with fetched file content (from github-data-file-fetcher)"),
]

_output_db_option = [
    click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
                 help="Output database for classification results (default: validated.db)"),
    click.option("--content-dir", type=click.Path(path_type=Path), default=Path("content"),
                 help="Directory with fetched file content"),
]

_llm_options = [
    click.option("--model", default=None,
                 help="LLM model name (default: claude-haiku-4-5-20251001)"),
    click.option("--base-url", default=None,
                 help="Base URL for API proxy (e.g. http://localhost:11434/v1 for Ollama)"),
    click.option("--concurrency", default=8, type=int,
                 help="Number of concurrent API requests (default: 8)"),
    click.option("--backend", type=click.Choice(["anthropic", "openai", "claude-agent-sdk"]),
                 default="anthropic",
                 help="API backend: 'anthropic' (per-token), 'openai' (Ollama/local), 'claude-agent-sdk' (subscription)"),
]


def _apply_options(options):
    """Decorator that applies a list of click.option decorators."""
    def decorator(func):
        for option in reversed(options):
            func = option(func)
        return func
    return decorator


def _make_args(**kwargs):
    """Convert click kwargs to an args namespace."""
    return types.SimpleNamespace(**kwargs)


# ============================================================
# Main pipeline: passes 1-3 (no LLM dependency)
# ============================================================

@cli.command("filter-pass-1")
@_apply_options(_filter_common_options)
def filter_pass1_cmd(main_db, output_db, content_dir):
    """Pass 1: Check YAML frontmatter.

    Scans all files in --main-db, reads content from --content-dir, and
    checks for valid YAML frontmatter. Results cached in validation_results
    table -- re-runs skip already-checked URLs.

    Files without frontmatter are rejected (has_frontmatter=0).
    """
    from .filter import filter_pass1
    asyncio.run(filter_pass1(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))


@cli.command("filter-pass-2")
@_apply_options(_filter_common_options)
def filter_pass2_cmd(main_db, output_db, content_dir):
    """Pass 2: Apply heuristic rejection rules.

    Runs deterministic rules on all files with frontmatter. Re-runs every
    time (rules may change). Catches: prompt injection patterns, academic
    papers (skillXiv/arxiv), blog posts, issue templates, commercial
    content, non-Claude platform tools, and empty files.

    Sets heuristic_reject=1 for definite non-skills. heuristic_reject=0
    means "not rejected by heuristics" (uncertain, not confirmed skill).
    """
    from .filter.heuristics import heuristic_reject
    from .filter.filter import init_output_db, open_db, scan_content, resolve_content_path
    from .filter.parse_github_url import parse_github_url
    from tqdm import tqdm

    init_output_db(output_db)
    _, to_validate, _ = scan_content(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    ))

    conn = open_db(output_db)
    has_fm = set(
        row[0] for row in conn.execute(
            "SELECT url FROM validation_results WHERE has_frontmatter = 1"
        ).fetchall()
    )
    conn.close()

    urls = [url for url in to_validate if url in has_fm]
    print(f"  Running heuristics on {len(urls):,} URLs...")

    conn = open_db(output_db)
    rejected = 0
    checked = 0
    for url in tqdm(urls, desc="Pass 2: heuristics", unit="url"):
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        is_rejected, reason = heuristic_reject(content)
        conn.execute(
            "UPDATE validation_results SET heuristic_reject = ?, heuristic_reason = ? WHERE url = ?",
            (1 if is_rejected else 0, reason if reason else None, url)
        )
        if is_rejected:
            rejected += 1
        checked += 1
        if checked % 5000 == 0:
            conn.commit()

    conn.commit()
    conn.close()
    print(f"  Checked: {checked:,}, Rejected: {rejected:,}")


@cli.command("filter-pass-3")
@_apply_options(_output_db_option)
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("data/labeled.csv"),
              help="Labeled training data CSV with 'url' and 'is_skill' columns")
def filter_pass3_cmd(output_db, content_dir, labeled_csv):
    """Pass 3: Train SVM classifier and predict on all files.

    Trains an SVM-rbf classifier on labeled data (from --labeled-csv),
    then predicts is_skill and confidence for all files with frontmatter.
    Processes in 10K batches to avoid OOM.

    Features: TF-IDF (1000 bigrams) + heuristic features (51) + URL
    features (8) + frontmatter key bag-of-words.

    Writes embedding_is_skill (0/1) and embedding_confidence (0.0-1.0)
    to validation_results. Confidence = distance from decision boundary.

    The classifier retrains from scratch every run (training is instant,
    labeled data may have changed).
    """
    from .filter.classify import classify_pass
    asyncio.run(classify_pass(_make_args(
        output_db=output_db, content_dir=content_dir,
        labeled_csv=labeled_csv,
    )))


@cli.command("filter-valid-skills")
@_apply_options(_filter_common_options)
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("data/labeled.csv"),
              help="Labeled training data CSV")
def filter_valid_skills(main_db, output_db, content_dir, labeled_csv):
    """Run the full classification pipeline (passes 1-3).

    No LLM required. Runs frontmatter check, heuristic rejection, and
    SVM classification in sequence. Results written to --output-db with
    confidence scores for downstream filtering.
    """
    from .filter import filter_pass1
    from .filter.classify import classify_pass
    from .filter.heuristics import heuristic_reject
    from .filter.filter import init_output_db, open_db, scan_content, resolve_content_path
    from .filter.parse_github_url import parse_github_url
    from tqdm import tqdm

    args_common = _make_args(main_db=main_db, output_db=output_db, content_dir=content_dir)

    print("=== Pass 1: Frontmatter ===")
    asyncio.run(filter_pass1(args_common))

    print("\n=== Pass 2: Heuristics ===")
    _, to_validate, _ = scan_content(args_common)
    conn = open_db(output_db)
    has_fm = set(row[0] for row in conn.execute(
        "SELECT url FROM validation_results WHERE has_frontmatter = 1"
    ).fetchall())
    conn.close()
    urls = [url for url in to_validate if url in has_fm]
    conn = open_db(output_db)
    for url in tqdm(urls, desc="Pass 2: heuristics", unit="url"):
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        is_rejected, reason = heuristic_reject(content)
        conn.execute(
            "UPDATE validation_results SET heuristic_reject = ?, heuristic_reason = ? WHERE url = ?",
            (1 if is_rejected else 0, reason if reason else None, url)
        )
    conn.commit()
    conn.close()

    print("\n=== Pass 3: Classify ===")
    asyncio.run(classify_pass(_make_args(
        output_db=output_db, content_dir=content_dir,
        labeled_csv=labeled_csv,
    )))


# ============================================================
# Training data generation (LLM, offline)
# ============================================================

@cli.command("generate-training-data")
@_apply_options(_filter_common_options + _llm_options)
@click.option("--limit", default=None, type=int,
              help="Process at most N URLs (default: all)")
@click.option("--confidence-threshold", default=0.5, type=float,
              help="Send URLs with classifier confidence below this to the LLM (default: 0.5)")
def generate_training_data(main_db, output_db, content_dir, model, base_url, concurrency, backend,
                           limit, confidence_threshold):
    """Send low-confidence URLs to an LLM for labeling.

    NOT part of the main pipeline. This command generates training data
    to improve the classifier. It reads URLs where the SVM classifier
    had low confidence (below --confidence-threshold), sends them to
    the specified LLM backend, and stores results in llm_skill_evaluation.

    After running, regenerate labeled.csv and re-run filter-pass-3 to
    retrain the classifier with the new labels.

    Uses file-based caching (cachetta) to avoid re-classifying content
    that has already been seen. Safe to interrupt and resume.

    Example workflow:
      1. skills-dataset filter-valid-skills ...    # main pipeline
      2. skills-dataset generate-training-data ... # label uncertain files
      3. python /tmp/gen_labeled_csv.py            # regenerate CSV
      4. skills-dataset filter-pass-3 ...          # retrain classifier
    """
    from .filter import filter_pass2
    asyncio.run(filter_pass2(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=limit, confidence_threshold=confidence_threshold,
    )))


# ============================================================
# Export
# ============================================================

@cli.command()
@click.option("--main-db", type=click.Path(path_type=Path), required=True,
              help="Source database with files/repos/history (from github-data-file-fetcher)")
@click.option("--validation-db", type=click.Path(path_type=Path), default=Path("validated.db"),
              help="Validation database with classification results (default: validated.db)")
@click.option("--output-dir", type=click.Path(path_type=Path), default=Path("build"),
              help="Output directory for Parquet files (default: build/)")
@click.option("--kaggle-username", help="Kaggle username for metadata generation")
@click.option("--allow-no-repo", is_flag=True, default=False,
              help="Allow export even if some valid files have no repo metadata")
@click.option("--allow-no-history", is_flag=True, default=False,
              help="Allow export even if some valid files have no commit history")
@click.option("--model", default=None,
              help="Use only this LLM model's evaluations (default: any)")
def export(main_db, validation_db, output_dir, kaggle_username, allow_no_repo, allow_no_history, model):
    """Export validated skills to Parquet for Kaggle.

    Reads classification results from --validation-db and exports files,
    repos, and history to Parquet format. Uses the SVM classifier results
    (embedding_is_skill) from validation_results, excluding heuristic
    rejects. Confidence scores included in output.
    """
    from .export import main as export_main
    export_main(_make_args(
        main_db=main_db, validation_db=validation_db, output_dir=output_dir,
        kaggle_username=kaggle_username, allow_no_repo=allow_no_repo,
        allow_no_history=allow_no_history, model=model,
    ))


if __name__ == "__main__":
    cli()
