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
    "--batch-size",
    type=int,
    default=100,
    help="URLs per processing batch (files are packed into API calls automatically)",
)
@click.option(
    "--max-concurrent",
    type=int,
    default=3,
    help="Max concurrent API calls",
)
@click.option(
    "--model",
    default=None,
    help="Claude model to use (default: claude-haiku-4-5-20251001)",
)
def filter_valid_skills(main_db, output_db, content_dir, batch_size, max_concurrent, model):
    """Filter SKILL.md files using Claude, producing a DB with only valid skills."""
    from .filter import main as filter_main

    class Args:
        pass

    args = Args()
    args.main_db = main_db
    args.output_db = output_db
    args.content_dir = content_dir
    args.batch_size = batch_size
    args.max_concurrent = max_concurrent
    args.model = model

    asyncio.run(filter_main(args))


@cli.command()
@click.option(
    "--db",
    type=click.Path(path_type=Path),
    default=Path("validated.db"),
    help="Database with valid skills (from filter-valid-skills)",
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
def export(db, output_dir, kaggle_username):
    """Export validated skills to Parquet for Kaggle."""
    from .export import main as export_main

    class Args:
        pass

    args = Args()
    args.db = db
    args.output_dir = output_dir
    args.kaggle_username = kaggle_username

    export_main(args)


if __name__ == "__main__":
    cli()
