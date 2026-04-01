"""CLI for GitHub Skills Dataset."""

import asyncio
import types

import click
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@click.group()
@click.version_option()
def cli():
    """GitHub Skills Dataset - Build SKILL.md dataset for Kaggle."""
    pass


# Shared options
_filter_common_options = [
    click.option("--main-db", type=click.Path(path_type=Path), required=True,
                 help="Source database from github-data-file-fetcher"),
    click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
                 help="Output database (default: validated.db)"),
    click.option("--content-dir", type=click.Path(path_type=Path), default=Path("content"),
                 help="Content directory from github-data-file-fetcher"),
]

_output_db_option = [
    click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
                 help="Output database (default: validated.db)"),
    click.option("--content-dir", type=click.Path(path_type=Path), default=Path("content"),
                 help="Content directory from github-data-file-fetcher"),
]

_filter_llm_options = [
    click.option("--model", default=None,
                 help="Claude model to use (default: claude-haiku-4-5-20251001)"),
    click.option("--base-url", default=None,
                 help="Base URL for API proxy (e.g. http://localhost:11434/v1)"),
    click.option("--concurrency", default=10, type=int,
                 help="Number of concurrent API requests (default: 10)"),
    click.option("--backend", type=click.Choice(["anthropic", "openai", "claude-agent-sdk"]),
                 default="anthropic",
                 help="API backend: 'anthropic' (default), 'openai' (for Ollama/local), 'claude-agent-sdk' (subscription)"),
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


# --- Pass 1: Frontmatter check ---

@cli.command("filter-pass-1")
@_apply_options(_filter_common_options)
def filter_pass1_cmd(main_db, output_db, content_dir):
    """Pass 1: reject files without valid YAML frontmatter."""
    from .filter import filter_pass1
    asyncio.run(filter_pass1(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))


# --- Pass 2: Heuristic reject ---

@cli.command("filter-pass-2")
@_apply_options(_filter_common_options)
def filter_pass2_cmd(main_db, output_db, content_dir):
    """Pass 2: reject files matching heuristic rules (instant, no LLM)."""
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


# --- Pass 3: Embed ---

@cli.command("filter-pass-3")
@_apply_options(_filter_common_options)
@click.option("--embedding-model", default="nomic-embed-text", help="Ollama embedding model")
@click.option("--ollama-url", default="http://localhost:11434", help="Ollama base URL")
def filter_pass3_cmd(main_db, output_db, content_dir, embedding_model, ollama_url):
    """Pass 3: generate embeddings for all files via Ollama."""
    from .filter.embed import embed_pass
    asyncio.run(embed_pass(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        embedding_model=embedding_model, ollama_url=ollama_url,
    )))


# --- Pass 4: Classify with embeddings ---

@cli.command("filter-pass-4")
@_apply_options(_output_db_option)
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("data/labeled.csv"),
              help="Labeled data CSV (default: data/labeled.csv)")
@click.option("--embedding-model", default="nomic-embed-text", help="Ollama embedding model")
@click.option("--confidence-threshold", default=None, type=float,
              help="Confidence threshold (auto-detected if not set)")
def filter_pass4_cmd(output_db, content_dir, labeled_csv, embedding_model, confidence_threshold):
    """Pass 4: train embedding classifier and predict on all files."""
    from .filter.classify import classify_pass
    asyncio.run(classify_pass(_make_args(
        output_db=output_db, content_dir=content_dir,
        labeled_csv=labeled_csv, embedding_model=embedding_model,
        confidence_threshold=confidence_threshold,
    )))


# --- Pass 5: LLM fallback ---

@cli.command("filter-pass-5")
@_apply_options(_filter_common_options + _filter_llm_options)
@click.option("--limit", default=None, type=int, help="Process at most N URLs (for testing)")
@click.option("--confidence-threshold", default=0.8, type=float,
              help="Only classify URLs with embedding confidence below this (default: 0.8)")
def filter_pass5_cmd(main_db, output_db, content_dir, model, base_url, concurrency, backend, limit, confidence_threshold):
    """Pass 5: LLM classification for low-confidence embeddings."""
    from .filter import filter_pass2
    asyncio.run(filter_pass2(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=limit, confidence_threshold=confidence_threshold,
    )))


# --- Combined ---

@cli.command("filter-valid-skills")
@_apply_options(_filter_common_options + _filter_llm_options)
@click.option("--embedding-model", default="nomic-embed-text", help="Ollama embedding model")
@click.option("--ollama-url", default="http://localhost:11434", help="Ollama base URL")
@click.option("--labeled-csv", type=click.Path(path_type=Path), default=Path("data/labeled.csv"))
@click.option("--confidence-threshold", default=None, type=float)
def filter_valid_skills(main_db, output_db, content_dir, model, base_url, concurrency, backend,
                        embedding_model, ollama_url, labeled_csv, confidence_threshold):
    """Run all 5 filter passes in sequence."""
    from .filter import filter_pass1, filter_pass2
    from .filter.embed import embed_pass
    from .filter.classify import classify_pass

    args_common = _make_args(main_db=main_db, output_db=output_db, content_dir=content_dir)

    print("=== Pass 1: Frontmatter ===")
    asyncio.run(filter_pass1(args_common))

    print("\n=== Pass 2: Heuristics ===")
    # Invoke pass 2 inline (same logic as CLI command)
    from .filter.heuristics import heuristic_reject
    from .filter.filter import init_output_db, open_db, scan_content, resolve_content_path
    from .filter.parse_github_url import parse_github_url
    from tqdm import tqdm

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

    print("\n=== Pass 3: Embed ===")
    asyncio.run(embed_pass(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        embedding_model=embedding_model, ollama_url=ollama_url,
    )))

    print("\n=== Pass 4: Classify ===")
    asyncio.run(classify_pass(_make_args(
        output_db=output_db, content_dir=content_dir,
        labeled_csv=labeled_csv, embedding_model=embedding_model,
        confidence_threshold=confidence_threshold,
    )))

    print("\n=== Pass 5: LLM fallback ===")
    asyncio.run(filter_pass2(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=None, confidence_threshold=confidence_threshold or 0.8,
    )))


# --- Export ---

@cli.command()
@click.option("--main-db", type=click.Path(path_type=Path), required=True,
              help="Source database with files/repos/history (from github-data-file-fetcher)")
@click.option("--validation-db", type=click.Path(path_type=Path), default=Path("validated.db"),
              help="Validation database (default: validated.db)")
@click.option("--output-dir", type=click.Path(path_type=Path), default=Path("build"),
              help="Output directory (default: build/)")
@click.option("--kaggle-username", help="Kaggle username for metadata generation")
@click.option("--allow-no-repo", is_flag=True, default=False,
              help="Allow export even if some valid files have no repo metadata")
@click.option("--allow-no-history", is_flag=True, default=False,
              help="Allow export even if some valid files have no commit history")
@click.option("--model", default=None,
              help="Use only this model's evaluations (default: any model)")
def export(main_db, validation_db, output_dir, kaggle_username, allow_no_repo, allow_no_history, model):
    """Export validated skills to Parquet for Kaggle."""
    from .export import main as export_main
    export_main(_make_args(
        main_db=main_db, validation_db=validation_db, output_dir=output_dir,
        kaggle_username=kaggle_username, allow_no_repo=allow_no_repo,
        allow_no_history=allow_no_history, model=model,
    ))


# --- Legacy aliases ---

@cli.command("filter-valid-skills-pass-1", hidden=True)
@_apply_options(_filter_common_options)
def filter_pass1_legacy(main_db, output_db, content_dir):
    """Legacy alias for filter-pass-1."""
    from .filter import filter_pass1
    asyncio.run(filter_pass1(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))


@cli.command("filter-valid-skills-pass-2", hidden=True)
@_apply_options(_filter_common_options + _filter_llm_options)
@click.option("--limit", default=None, type=int)
def filter_pass2_legacy(main_db, output_db, content_dir, model, base_url, concurrency, backend, limit):
    """Legacy alias for filter-pass-5."""
    from .filter import filter_pass2
    asyncio.run(filter_pass2(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=limit,
    )))


if __name__ == "__main__":
    cli()
