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


# Shared options for filter commands
_filter_common_options = [
    click.option("--main-db", type=click.Path(path_type=Path), required=True,
                 help="Source database from github-data-file-fetcher"),
    click.option("--output-db", type=click.Path(path_type=Path), default=Path("validated.db"),
                 help="Output database with valid skills only (default: validated.db)"),
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
    click.option("--backend", type=click.Choice(["anthropic", "claude-agent-sdk"]),
                 default="anthropic",
                 help="API backend: 'anthropic' for per-token billing, 'claude-agent-sdk' for subscription billing"),
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


@cli.command("filter-valid-skills-pass-1")
@_apply_options(_filter_common_options)
def filter_pass1_cmd(main_db, output_db, content_dir):
    """Pass 1: reject files without valid YAML frontmatter (free, no LLM)."""
    from .filter import filter_pass1
    asyncio.run(filter_pass1(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))


@cli.command("filter-valid-skills-pass-2")
@_apply_options(_filter_common_options + _filter_llm_options)
@click.option("--limit", default=None, type=int, help="Process at most N URLs (for testing)")
def filter_pass2_cmd(main_db, output_db, content_dir, model, base_url, concurrency, backend, limit):
    """Pass 2: classify files with valid frontmatter via LLM."""
    from .filter import filter_pass2
    asyncio.run(filter_pass2(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
        limit=limit,
    )))


@cli.command("filter-valid-skills")
@_apply_options(_filter_common_options + _filter_llm_options)
def filter_valid_skills(main_db, output_db, content_dir, model, base_url, concurrency, backend):
    """Filter SKILL.md files: frontmatter check (pass 1) then LLM classification (pass 2)."""
    from .filter import filter
    asyncio.run(filter(_make_args(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model=model, base_url=base_url, concurrency=concurrency, backend=backend,
    )))


@cli.command()
@click.option("--main-db", type=click.Path(path_type=Path), required=True,
              help="Source database with files/repos/history (from github-data-file-fetcher)")
@click.option("--validation-db", type=click.Path(path_type=Path), default=Path("validated.db"),
              help="Validation database with is_skill verdicts (default: validated.db)")
@click.option("--output-dir", type=click.Path(path_type=Path), default=Path("build"),
              help="Output directory (default: build/)")
@click.option("--kaggle-username", help="Kaggle username for metadata generation")
@click.option("--allow-no-repo", is_flag=True, default=False,
              help="Allow export even if some valid files have no repo metadata")
@click.option("--allow-no-history", is_flag=True, default=False,
              help="Allow export even if some valid files have no commit history")
def export(main_db, validation_db, output_dir, kaggle_username, allow_no_repo, allow_no_history):
    """Export validated skills to Parquet for Kaggle."""
    from .export import main as export_main
    export_main(_make_args(
        main_db=main_db, validation_db=validation_db, output_dir=output_dir,
        kaggle_username=kaggle_username, allow_no_repo=allow_no_repo,
        allow_no_history=allow_no_history,
    ))


if __name__ == "__main__":
    cli()
