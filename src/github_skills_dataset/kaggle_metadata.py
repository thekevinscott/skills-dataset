"""Generate Kaggle dataset metadata."""

import json
from pathlib import Path

def generate_metadata(output_dir: Path, username: str, files_count: int, repos_count: int):
    """Generate dataset-metadata.json and README.md."""

    metadata = {
        "title": "GitHub SKILL.md Files - Claude Code Skills",
        "id": f"{username}/github-skill-files",
        "licenses": [{"name": "CC0-1.0"}],
        "keywords": ["github", "claude", "skills", "ai", "automation", "claude-code"],
        "description": "SKILL.md files from GitHub repositories, with repository metadata and commit history.",
        "resources": [
            {"path": "files.parquet", "description": "File URLs and basic Git info"},
            {"path": "repos.parquet", "description": "Repository metadata (stars, forks, language, topics)"},
            {"path": "history.parquet", "description": "Commit history (first/last dates, total commits)"},
        ]
    }

    (output_dir / "dataset-metadata.json").write_text(json.dumps(metadata, indent=2))

    readme = """# GitHub SKILL.md Files Dataset

SKILL.md files from GitHub repositories.

## Files

### files.parquet
- `url`: GitHub blob URL (primary key)
- `sha`: Git commit SHA
- `filename`: File name (e.g., "SKILL.md")
- `path`: Path in repository
- `repo_key`: Foreign key to repos (owner/repo)
- `size_bytes`: File size in bytes
- `discovered_at`: When we collected this file

### repos.parquet
- `repo_key`: owner/repo (primary key)
- `repo_owner`: GitHub username/org
- `repo_name`: Repository name
- `stars`: Stargazers count
- `forks`: Fork count
- `watchers`: Watchers count
- `language`: Primary language
- `topics`: Array of topics
- `description`: Repository description
- `license`: SPDX license ID
- `created_at`, `updated_at`: Timestamps

### history.parquet
One row per commit per file.
- `url`: File URL (foreign key to files)
- `commit_sha`: Short commit SHA
- `commit_author`: Author name
- `commit_date`: ISO 8601 timestamp
- `commit_message`: Commit message

## Usage

```python
import polars as pl

files = pl.read_parquet("files.parquet")
repos = pl.read_parquet("repos.parquet")
history = pl.read_parquet("history.parquet")

# Join files with repos
df = files.join(repos, on="repo_key")
```

## Data Collection

1. **Discovery**: File paths collected via GitHub code search API using [github-data-file-fetcher](https://github.com/thekevinscott/github-data-file-fetcher)
2. **Filtering**: Claude-based semantic classification to identify genuine skill files
3. **Metadata**: Repository metadata and commit history fetched via GitHub GraphQL API
4. **Export**: 3 normalized Parquet files

## License

CC0-1.0 (Public Domain)
"""

    (output_dir / "README.md").write_text(readme)
