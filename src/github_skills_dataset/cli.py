"""CLI for GitHub Skills Dataset."""

import asyncio
import click
from pathlib import Path


@click.group()
@click.version_option()
def cli():
    """GitHub Skills Dataset - Build validated SKILL.md dataset for Kaggle."""
    pass


@cli.command()
@click.option(
    "--main-db",
    type=click.Path(path_type=Path),
    default=Path("results/skills_v3.db"),
    help="Main database from github-data-file-fetcher",
)
@click.option(
    "--validation-db",
    type=click.Path(path_type=Path),
    default=Path("validation.db"),
    help="Validation results database",
)
@click.option(
    "--content-dir",
    type=click.Path(path_type=Path),
    default=Path("results/content"),
    help="Content directory from github-data-file-fetcher",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="Files per batch",
)
@click.option(
    "--max-concurrent",
    type=int,
    default=3,
    help="Max concurrent API calls",
)
def validate(main_db, validation_db, content_dir, batch_size, max_concurrent):
    """Validate SKILL.md files using Claude."""
    from .validate import main as validate_main

    # Create a mock args object
    class Args:
        pass

    args = Args()
    args.main_db = main_db
    args.validation_db = validation_db
    args.content_dir = content_dir
    args.batch_size = batch_size
    args.max_concurrent = max_concurrent

    # Run the validation with our args
    asyncio.run(validate_main(args))


@cli.command()
@click.option(
    "--main-db",
    type=click.Path(path_type=Path),
    default=Path("results/skills_v3.db"),
    help="Main database from github-data-file-fetcher",
)
@click.option(
    "--validation-db",
    type=click.Path(path_type=Path),
    default=Path("validation.db"),
    help="Validation results database",
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
def export(main_db, validation_db, output_dir, kaggle_username):
    """Export validated skills to Parquet for Kaggle."""
    from .export import main as export_main

    # Create a mock args object
    class Args:
        pass

    args = Args()
    args.main_db = main_db
    args.validation_db = validation_db
    args.output_dir = output_dir
    args.kaggle_username = kaggle_username

    # Run the export with our args
    export_main(args)


if __name__ == "__main__":
    cli()
