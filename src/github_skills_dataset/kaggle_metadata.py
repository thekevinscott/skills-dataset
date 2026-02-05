"""Generate Kaggle dataset metadata."""

import json
from pathlib import Path

def generate_metadata(output_dir: Path, username: str, files_count: int, repos_count: int):
    """Generate dataset-metadata.json and README.md."""

    # Generate dataset-metadata.json
    metadata = {
        "title": "GitHub SKILL.md Files - Claude Code Skills",
        "id": f"{username}/github-skill-files",
        "licenses": [{"name": "CC0-1.0"}],
        "keywords": ["github", "claude", "skills", "ai", "automation", "claude-code"],
        "description": f"Validated SKILL.md files from {repos_count:,} GitHub repositories. "
                       f"Contains {files_count:,} skill files with repository metadata and commit history.",
        "resources": [
            {"path": "files.parquet", "description": "File URLs and basic Git info"},
            {"path": "repos.parquet", "description": "Repository metadata (stars, forks, language, topics)"},
            {"path": "history.parquet", "description": "Commit history (first/last dates, total commits)"},
            {"path": "scripts/", "description": "Validation and export scripts for reproducibility"},
        ]
    }

    (output_dir / "dataset-metadata.json").write_text(json.dumps(metadata, indent=2))

    # Generate README.md
    readme = f"""# GitHub SKILL.md Files Dataset

Validated SKILL.md files from {repos_count:,} GitHub repositories.

## Contents

- **{files_count:,} validated skill files** from GitHub
- **{repos_count:,} repositories** with metadata (stars, forks, topics, language)
- **Commit history** showing when files were created and last modified

## Files

### files.parquet
Basic file information from Git:
- `url`: GitHub blob URL (primary key)
- `sha`: Git commit SHA
- `filename`: File name (e.g., "SKILL.md")
- `path`: Path in repository
- `repo_key`: Foreign key to repos (owner/repo)
- `size_bytes`: File size in bytes
- `discovered_at`: When we collected this file

### repos.parquet
Repository-level metadata from GitHub:
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
- `url`: File URL (foreign key to files)
- `first_commit_date`: When file was created
- `last_commit_date`: Most recent commit
- `total_commits`: Number of commits

## Usage

```python
import polars as pl

# Load data
files = pl.read_parquet("files.parquet")
repos = pl.read_parquet("repos.parquet")
history = pl.read_parquet("history.parquet")

# Join files with repos
df = files.join(repos, on="repo_key")

# Filter high-quality repos
df = df.filter(pl.col("stars") > 100)
```

## Data Collection

1. **Collection**: Files collected using [github-data-file-fetcher](https://github.com/yourusername/github-data-file-fetcher)
2. **Validation**: Two-pass validation:
   - First pass: Check for valid YAML frontmatter
   - Second pass: Claude-based semantic validation
3. **Export**: 3 normalized Parquet files

See `scripts/` folder for complete validation and export code.

## License

CC0-1.0 (Public Domain)
"""

    (output_dir / "README.md").write_text(readme)
