"""Export validated skills to Parquet for Kaggle."""

import polars as pl
import sqlite3
from pathlib import Path

def get_validated_urls(validation_db: Path) -> set[str]:
    """Get URLs where is_skill=true."""
    conn = sqlite3.connect(validation_db)
    cursor = conn.execute("SELECT url FROM validation_results WHERE is_skill = 1")
    urls = {row[0] for row in cursor.fetchall()}
    conn.close()
    return urls

def export_files(main_db: Path, validation_db: Path, output_path: Path):
    """Export files.parquet."""
    valid_urls = get_validated_urls(validation_db)

    # Read files from main DB
    conn = sqlite3.connect(main_db)
    df = pl.read_database("SELECT url, sha, size_bytes, discovered_at FROM files", conn)
    conn.close()

    # Filter to validated files
    df = df.filter(pl.col("url").is_in(list(valid_urls)))

    # Extract repo_key, filename, path
    df = df.with_columns([
        # Extract repo_key (owner/repo)
        pl.col("url").str.extract(r'github\.com/([^/]+/[^/]+)/', 1).alias("repo_key"),
        # Extract filename (last segment)
        pl.col("url").str.split("/").list.get(-1).alias("filename"),
        # Extract path (everything after blob/ref/)
        pl.col("url").str.extract(r'blob/[^/]+/(.+)$', 1).alias("path"),
    ])

    # Write files.parquet
    df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(df)

def export_repos(main_db: Path, files_df: pl.DataFrame, output_path: Path):
    """Export repos.parquet."""
    # Get unique repo_keys from files
    repo_keys = files_df.select("repo_key").unique()

    # Read repo_metadata from main DB
    conn = sqlite3.connect(main_db)
    repos_df = pl.read_database("SELECT * FROM repo_metadata", conn)
    conn.close()

    # Join to filter only repos in our dataset
    repos_df = repos_df.join(repo_keys, left_on="repo_key", right_on="repo_key", how="inner")

    # Parse topics JSON to list
    repos_df = repos_df.with_columns([
        pl.col("topics").str.json_decode(pl.List(pl.Utf8)).alias("topics"),
        pl.col("repo_key").str.split("/").list.get(0).alias("repo_owner"),
        pl.col("repo_key").str.split("/").list.get(1).alias("repo_name"),
    ])

    # Write repos.parquet
    repos_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(repos_df)

def export_history(main_db: Path, files_df: pl.DataFrame, output_path: Path):
    """Export history.parquet."""
    # Get URLs from files
    file_urls = files_df.select("url")

    # Read file_history from main DB
    conn = sqlite3.connect(main_db)
    history_df = pl.read_database("SELECT url, commits FROM file_history", conn)
    conn.close()

    # Join to filter only our files (left join - nullable)
    history_df = file_urls.join(history_df, on="url", how="left")

    # Parse commits JSON and extract stats
    commit_dtype = pl.List(pl.Struct({"sha": pl.Utf8, "author": pl.Utf8, "date": pl.Utf8, "message": pl.Utf8}))
    history_df = history_df.with_columns([
        pl.col("commits").str.json_decode(commit_dtype).alias("commits_array")
    ]).with_columns([
        # First commit (oldest) is last in array
        pl.col("commits_array").list.get(-1).struct.field("date").alias("first_commit_date"),
        # Last commit (newest) is first in array
        pl.col("commits_array").list.get(0).struct.field("date").alias("last_commit_date"),
        # Total commits
        pl.col("commits_array").list.len().alias("total_commits"),
    ]).drop("commits", "commits_array")

    # Write history.parquet
    history_df.write_parquet(output_path, compression="snappy", use_pyarrow=True)
    return len(history_df)

def main(args):
    """Main export pipeline."""

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Export files.parquet
    print("Exporting files.parquet...")
    files_count = export_files(
        args.main_db,
        args.validation_db,
        args.output_dir / "files.parquet"
    )
    print(f"  {files_count:,} files")

    # Read files back for join operations
    files_df = pl.read_parquet(args.output_dir / "files.parquet")

    # Export repos.parquet
    print("Exporting repos.parquet...")
    repos_count = export_repos(
        args.main_db,
        files_df,
        args.output_dir / "repos.parquet"
    )
    print(f"  {repos_count:,} repos")

    # Export history.parquet
    print("Exporting history.parquet...")
    export_history(
        args.main_db,
        files_df,
        args.output_dir / "history.parquet"
    )
    print(f"  {files_count:,} files (with nullable history)")

    # Generate Kaggle metadata
    if args.kaggle_username:
        from .kaggle_metadata import generate_metadata
        generate_metadata(args.output_dir, args.kaggle_username, files_count, repos_count)
        print(f"\n✅ Generated Kaggle metadata in {args.output_dir}")

    # Copy source package to output for reproducibility
    print("Copying source code to output...")
    scripts_dir = args.output_dir / "scripts"
    scripts_dir.mkdir(exist_ok=True)

    # Copy the entire src/ directory
    from shutil import copytree, copy2
    import os

    # Get the package root (2 levels up from this file)
    package_root = Path(__file__).parent.parent.parent
    src_dir = package_root / "src"

    # Copy src/ to scripts/src/
    if src_dir.exists():
        copytree(src_dir, scripts_dir / "src", dirs_exist_ok=True)

    # Copy pyproject.toml and README.md from package root
    for filename in ["pyproject.toml", "README.md"]:
        src_file = package_root / filename
        if src_file.exists():
            copy2(src_file, scripts_dir / filename)

    print(f"\n✅ Export complete: {args.output_dir}")
    print(f"   - 3 Parquet files")
    print(f"   - Kaggle metadata")
    print(f"   - Scripts for reproducibility")
