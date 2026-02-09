"""Export validated skills to Parquet for Kaggle."""

import polars as pl
import sqlite3
from pathlib import Path


class MissingDataError(Exception):
    """Raised when valid files lack expected repo metadata or history."""
    pass


def load_valid_urls(validation_db: Path) -> pl.DataFrame:
    """Read validated URLs from the validation database."""
    conn = sqlite3.connect(validation_db)
    df = pl.read_database(
        "SELECT url FROM validation_results WHERE is_skill = 1", conn
    )
    conn.close()
    return df


def export_files(main_db: Path, valid_urls_df: pl.DataFrame, output_path: Path):
    """Export files.parquet -- files from main_db filtered to valid URLs."""
    conn = sqlite3.connect(main_db)
    df = pl.read_database("SELECT url, sha, size_bytes, discovered_at FROM files", conn)
    conn.close()

    df = df.join(valid_urls_df, on="url", how="semi")

    df = df.with_columns([
        pl.col("url").str.extract(r'github\.com/([^/]+/[^/]+)/', 1).alias("repo_key"),
        pl.col("url").str.split("/").list.get(-1).alias("filename"),
        pl.col("url").str.extract(r'blob/[^/]+/(.+)$', 1).alias("path"),
    ])

    df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return df


def export_repos(main_db: Path, files_df: pl.DataFrame, output_path: Path, *, allow_missing: bool = False):
    """Export repos.parquet. Raises MissingDataError if repos are missing unless allow_missing."""
    needed_keys = files_df.select("repo_key").unique()

    conn = sqlite3.connect(main_db)
    repos_df = pl.read_database("SELECT * FROM repo_metadata", conn)
    conn.close()

    repos_df = repos_df.join(needed_keys, on="repo_key", how="semi")

    # Check coverage
    have_keys = repos_df.select("repo_key").unique()
    missing = needed_keys.join(have_keys, on="repo_key", how="anti")
    if len(missing) > 0:
        sample = missing.head(10)["repo_key"].to_list()
        msg = f"{len(missing):,} valid files have no repo metadata (e.g. {', '.join(sample)})"
        if not allow_missing:
            raise MissingDataError(f"{msg}\nUse --allow-no-repo to export anyway.")
        print(f"  WARNING: {msg}")

    repos_df = repos_df.with_columns([
        pl.col("topics").str.json_decode(pl.List(pl.Utf8)).alias("topics"),
        pl.col("repo_key").str.split("/").list.get(0).alias("repo_owner"),
        pl.col("repo_key").str.split("/").list.get(1).alias("repo_name"),
    ])

    repos_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(repos_df)


def export_history(main_db: Path, files_df: pl.DataFrame, output_path: Path, *, allow_missing: bool = False):
    """Export history.parquet. Raises MissingDataError if history is missing unless allow_missing."""
    file_urls = files_df.select("url")

    conn = sqlite3.connect(main_db)
    history_df = pl.read_database("SELECT url, commits FROM file_history", conn)
    conn.close()

    # Check coverage before joining
    have_urls = history_df.select("url").unique()
    missing = file_urls.join(have_urls, on="url", how="anti")
    if len(missing) > 0:
        sample = missing.head(10)["url"].to_list()
        msg = f"{len(missing):,} valid files have no history (e.g. {sample[0]})"
        if not allow_missing:
            raise MissingDataError(f"{msg}\nUse --allow-no-history to export anyway.")
        print(f"  WARNING: {msg}")

    history_df = file_urls.join(history_df, on="url", how="left")

    commit_dtype = pl.List(pl.Struct({"sha": pl.Utf8, "author": pl.Utf8, "date": pl.Utf8, "message": pl.Utf8}))
    history_df = history_df.with_columns(
        pl.col("commits").str.json_decode(commit_dtype).alias("commits_parsed")
    ).drop("commits").explode("commits_parsed").unnest("commits_parsed").rename({
        "sha": "commit_sha",
        "author": "commit_author",
        "date": "commit_date",
        "message": "commit_message",
    })

    history_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(history_df)


def main(args):
    """Main export pipeline."""
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading valid URLs from validation DB...")
    valid_urls_df = load_valid_urls(args.validation_db)
    print(f"  {len(valid_urls_df):,} valid skill URLs")

    print("Exporting files.parquet...")
    files_df = export_files(args.main_db, valid_urls_df, args.output_dir / "files.parquet")
    files_count = len(files_df)
    print(f"  {files_count:,} files")

    print("Exporting repos.parquet...")
    repos_count = export_repos(
        args.main_db, files_df, args.output_dir / "repos.parquet",
        allow_missing=args.allow_no_repo,
    )
    print(f"  {repos_count:,} repos")

    print("Exporting history.parquet...")
    history_count = export_history(
        args.main_db, files_df, args.output_dir / "history.parquet",
        allow_missing=args.allow_no_history,
    )
    print(f"  {history_count:,} history entries")

    if args.kaggle_username:
        from .kaggle_metadata import generate_metadata
        generate_metadata(args.output_dir, args.kaggle_username, files_count, repos_count)

    # Copy source package to output for reproducibility
    print("Copying source code...")
    scripts_dir = args.output_dir / "scripts"
    scripts_dir.mkdir(exist_ok=True)

    from shutil import copytree, copy2
    package_root = Path(__file__).parent.parent.parent
    src_dir = package_root / "src"

    if src_dir.exists():
        copytree(src_dir, scripts_dir / "src", dirs_exist_ok=True)

    for filename in ["pyproject.toml", "README.md"]:
        src_file = package_root / filename
        if src_file.exists():
            copy2(src_file, scripts_dir / filename)

    print(f"\nDone: {args.output_dir}")
