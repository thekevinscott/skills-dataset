"""Export validated skills to Parquet for Kaggle."""

import polars as pl
import sqlite3
from pathlib import Path


def export_files(db: Path, output_path: Path):
    """Export files.parquet."""
    conn = sqlite3.connect(db)
    df = pl.read_database("SELECT url, sha, size_bytes, discovered_at FROM files", conn)
    conn.close()

    df = df.with_columns([
        pl.col("url").str.extract(r'github\.com/([^/]+/[^/]+)/', 1).alias("repo_key"),
        pl.col("url").str.split("/").list.get(-1).alias("filename"),
        pl.col("url").str.extract(r'blob/[^/]+/(.+)$', 1).alias("path"),
    ])

    df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(df)


def export_repos(db: Path, files_df: pl.DataFrame, output_path: Path):
    """Export repos.parquet."""
    repo_keys = files_df.select("repo_key").unique()

    conn = sqlite3.connect(db)
    repos_df = pl.read_database("SELECT * FROM repo_metadata", conn)
    conn.close()

    repos_df = repos_df.join(repo_keys, left_on="repo_key", right_on="repo_key", how="inner")

    repos_df = repos_df.with_columns([
        pl.col("topics").str.json_decode(pl.List(pl.Utf8)).alias("topics"),
        pl.col("repo_key").str.split("/").list.get(0).alias("repo_owner"),
        pl.col("repo_key").str.split("/").list.get(1).alias("repo_name"),
    ])

    repos_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(repos_df)


def export_history(db: Path, files_df: pl.DataFrame, output_path: Path):
    """Export history.parquet."""
    file_urls = files_df.select("url")

    conn = sqlite3.connect(db)
    history_df = pl.read_database("SELECT url, commits FROM file_history", conn)
    conn.close()

    history_df = file_urls.join(history_df, on="url", how="left")

    commit_dtype = pl.List(pl.Struct({"sha": pl.Utf8, "author": pl.Utf8, "date": pl.Utf8, "message": pl.Utf8}))
    history_df = history_df.with_columns([
        pl.col("commits").str.json_decode(commit_dtype).alias("commits_array")
    ]).with_columns([
        pl.col("commits_array").list.get(-1).struct.field("date").alias("first_commit_date"),
        pl.col("commits_array").list.get(0).struct.field("date").alias("last_commit_date"),
        pl.col("commits_array").list.len().alias("total_commits"),
    ]).drop("commits", "commits_array")

    history_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(history_df)


def main(args):
    """Main export pipeline."""
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("Exporting files.parquet...")
    files_count = export_files(args.db, args.output_dir / "files.parquet")
    print(f"  {files_count:,} files")

    files_df = pl.read_parquet(args.output_dir / "files.parquet")

    print("Exporting repos.parquet...")
    repos_count = export_repos(args.db, files_df, args.output_dir / "repos.parquet")
    print(f"  {repos_count:,} repos")

    print("Exporting history.parquet...")
    export_history(args.db, files_df, args.output_dir / "history.parquet")
    print(f"  {files_count:,} files (with nullable history)")

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
