"""CLI for GitHub Skills Dataset."""

import asyncio
import click
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@click.group()
@click.version_option()
def cli():
    """GitHub Skills Dataset - Build SKILL.md dataset for Kaggle."""
    pass


@cli.command("filter-valid-skills")
@click.option(
    "--main-db",
    type=click.Path(path_type=Path),
    required=True,
    help="Source database from github-data-file-fetcher",
)
@click.option(
    "--output-db",
    type=click.Path(path_type=Path),
    default=Path("validated.db"),
    help="Output database with valid skills only (default: validated.db)",
)
@click.option(
    "--content-dir",
    type=click.Path(path_type=Path),
    default=Path("content"),
    help="Content directory from github-data-file-fetcher",
)
@click.option(
    "--model",
    default=None,
    help="Claude model to use (default: claude-haiku-4-5-20251001)",
)
@click.option(
    "--base-url",
    default=None,
    help="Base URL for API proxy (e.g. http://localhost:11434/v1)",
)
@click.option(
    "--concurrency",
    default=10,
    type=int,
    help="Number of concurrent API requests (default: 10, use 1 for sequential)",
)
def filter_valid_skills(main_db, output_db, content_dir, model, base_url, concurrency):
    """Filter SKILL.md files using Claude Message Batches API (50% discount)."""
    from .filter import filter

    class Args:
        pass

    args = Args()
    args.main_db = main_db
    args.output_db = output_db
    args.content_dir = content_dir
    args.model = model
    args.base_url = base_url
    args.concurrency = concurrency

    asyncio.run(filter(args))


@cli.command()
@click.option(
    "--main-db",
    type=click.Path(path_type=Path),
    required=True,
    help="Source database with files/repos/history (from github-data-file-fetcher)",
)
@click.option(
    "--validation-db",
    type=click.Path(path_type=Path),
    default=Path("validated.db"),
    help="Validation database with is_skill verdicts (default: validated.db)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("build"),
    help="Output directory (default: build/)",
)
@click.option(
    "--kaggle-username",
    help="Kaggle username for metadata generation",
)
@click.option(
    "--allow-no-repo",
    is_flag=True,
    default=False,
    help="Allow export even if some valid files have no repo metadata",
)
@click.option(
    "--allow-no-history",
    is_flag=True,
    default=False,
    help="Allow export even if some valid files have no commit history",
)
def export(main_db, validation_db, output_dir, kaggle_username, allow_no_repo, allow_no_history):
    """Export validated skills to Parquet for Kaggle."""
    from .export import main as export_main

    class Args:
        pass

    args = Args()
    args.main_db = main_db
    args.validation_db = validation_db
    args.output_dir = output_dir
    args.kaggle_username = kaggle_username
    args.allow_no_repo = allow_no_repo
    args.allow_no_history = allow_no_history

    export_main(args)


if __name__ == "__main__":
    cli()
