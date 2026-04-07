"""CLI for GitHub Skills Dataset.

Pipeline:
  filter-pass-1        Frontmatter check (free, instant)
  filter-pass-2        Heuristic reject rules (free, instant)
  filter-pass-3        SVM classifier with confidence scores (free, ~15min)
  filter-valid-skills  Run all 3 passes in sequence

Training data:
  generate-training-data  Send unlabeled files to LLM for labeling

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
    from .filter.heuristics import heuristic_pass
    asyncio.run(heuristic_pass(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))


@cli.command("filter-pass-3")
@_apply_options(_output_db_option)
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("training/labeled.csv"),
              help="Labeled training data CSV with 'url' and 'is_skill' columns")
def filter_pass3_cmd(output_db, content_dir, labeled_csv):
    """Pass 3: Train SVM classifier and predict on all files.

    Trains an SVM-rbf classifier on labeled data (from --labeled-csv),
    then predicts is_skill and confidence for all files with frontmatter.
    Processes in 10K batches to avoid OOM.

    Features: TF-IDF (1000 bigrams) + heuristic features (51) + URL
    features (8) + frontmatter key bag-of-words.

    Writes classifier_is_skill (0/1) and classifier_confidence (0.0-1.0)
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
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("training/labeled.csv"),
              help="Labeled training data CSV")
def filter_valid_skills(main_db, output_db, content_dir, labeled_csv):
    """Run the full classification pipeline (passes 1-3).

    No LLM required. Runs frontmatter check, heuristic rejection, and
    SVM classification in sequence. Results written to --output-db with
    Results written to --output-db.
    """
    from .filter import filter_pass1
    from .filter.heuristics import heuristic_pass
    from .filter.classify import classify_pass

    args_common = _make_args(main_db=main_db, output_db=output_db, content_dir=content_dir)

    print("=== Pass 1: Frontmatter ===")
    asyncio.run(filter_pass1(args_common))

    print("\n=== Pass 2: Heuristics ===")
    asyncio.run(heuristic_pass(args_common))

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
              help="Process at most N content hashes (default: all)")
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("training/labeled.csv"),
              help="Training data CSV (default: training/labeled.csv)")
def generate_training_data(main_db, output_db, content_dir, model, base_url, concurrency, backend,
                           limit, labeled_csv):
    """Send unlabeled files to an LLM for classification.

    NOT part of the main pipeline. Generates training data to improve
    the classifier. Reads training/labeled.csv to skip already-labeled
    content, sends the rest to the LLM, appends results to the CSV.

    After running, re-run filter-pass-3 to retrain the classifier.

    Example workflow:
      1. skills-dataset filter-valid-skills ...     # main pipeline
      2. skills-dataset generate-training-data ...  # label new files
      3. skills-dataset filter-pass-3 ...           # retrain
    """
    from .filter.training import generate_training
    asyncio.run(generate_training(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        labeled_csv=labeled_csv,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=limit,
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
def export(main_db, validation_db, output_dir, kaggle_username, allow_no_repo, allow_no_history):
    """Export validated skills to Parquet for Kaggle.

    Reads classification results from --validation-db and exports files,
    repos, and history to Parquet format. Includes all files classified
    as skills by the SVM classifier, excluding heuristic rejects.
    """
    from .export import main as export_main
    export_main(_make_args(
        main_db=main_db, validation_db=validation_db, output_dir=output_dir,
        kaggle_username=kaggle_username, allow_no_repo=allow_no_repo,
        allow_no_history=allow_no_history,
    ))


@cli.command("prepare-for-fetcher")
@click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
              help="Validation database (default: validated.db)")
def prepare_for_fetcher(output_db):
    """Create a 'files' table in validated.db for github-data-file-fetcher.

    The fetcher (fetch-repo-metadata, fetch-file-history) expects a 'files'
    table with a 'url' column. This command populates it from the classifier
    results so you can point the fetcher at validated.db.

    Example:
      skills-dataset prepare-for-fetcher --output-db data/validated.db
      github-fetch fetch-repo-metadata --db data/validated.db
      github-fetch fetch-file-history --db data/validated.db
    """
    from .filter.filter import open_db

    conn = open_db(output_db)
    conn.execute("DROP TABLE IF EXISTS files")
    conn.execute("""
        CREATE TABLE files AS
        SELECT url FROM validation_results
        WHERE has_frontmatter = 1
        AND (heuristic_reject IS NULL OR heuristic_reject != 1)
        AND classifier_is_skill = 1
    """)

    count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    conn.commit()
    conn.close()
    print(f"Created 'files' table in {output_db}: {count:,} valid skill URLs")


@cli.command("hash")
@click.argument("file", type=click.Path(exists=True, path_type=Path), required=False)
@click.option("--text", default=None, help="Hash a text string (UTF-8 encoded) instead of a file")
def hash_cmd(file, text):
    """Compute the content hash of a file or text.

    The hash is sha256 of raw bytes. For files, this matches sha256sum.
    For --text, the string is UTF-8 encoded before hashing.

    Examples:
      skills-dataset hash SKILL.md
      skills-dataset hash --text "---\\nname: test\\n---"
      cat SKILL.md | skills-dataset hash /dev/stdin
    """
    from .hash import content_hash, content_hash_file

    if text is not None:
        print(content_hash(text.encode("utf-8")))
    elif file is not None:
        print(content_hash_file(file))
    else:
        raise click.UsageError("Provide a file path or --text")


if __name__ == "__main__":
    cli()
