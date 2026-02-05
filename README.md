# GitHub Skills Dataset

Build a validated SKILL.md dataset for Kaggle upload.

## Installation

```bash
uv sync
```

## Usage

### 1. Collect data

```bash
# Use github-data-file-fetcher to collect files
uvx github-data-file-fetcher fetch-file-paths "filename:SKILL.md"
uvx github-data-file-fetcher fetch-file-content
uvx github-data-file-fetcher fetch-repo-metadata
uvx github-data-file-fetcher fetch-file-history
```

### 2. Validate files

```bash
# Run validation (uses Claude SDK)
uv run skills-dataset validate
```

Options:
- `--main-db PATH` - Main database from github-data-file-fetcher (default: results/skills_v3.db)
- `--validation-db PATH` - Validation results database (default: validation.db)
- `--content-dir PATH` - Content directory (default: results/content)
- `--batch-size INT` - Files per batch (default: 10)
- `--max-concurrent INT` - Max concurrent API calls (default: 3)

### 3. Export to Parquet

```bash
# Generate Parquet files for Kaggle
uv run skills-dataset export --kaggle-username yourname
```

Options:
- `--main-db PATH` - Main database from github-data-file-fetcher
- `--validation-db PATH` - Validation results database
- `--output-dir PATH` - Output directory (default: build/)
- `--kaggle-username TEXT` - Kaggle username for metadata generation

### 4. Upload to Kaggle

```bash
cd build
kaggle datasets create -p .
```

## Project Structure

```
skills-dataset/                    # Git repo
  ├── src/
  │   └── github_skills_dataset/   # Python package
  │       ├── __init__.py
  │       ├── cli.py               # Click CLI
  │       ├── validate.py          # Validation logic
  │       ├── export.py            # Export logic
  │       └── kaggle_metadata.py   # Metadata generation
  ├── pyproject.toml
  ├── README.md
  ├── .gitignore
  ├── validation.db                # Generated (gitignored)
  ├── results/                     # From github-data-file-fetcher (gitignored)
  └── build/                       # Export output (gitignored)
      ├── files.parquet            # Dataset files
      ├── repos.parquet
      ├── history.parquet
      ├── dataset-metadata.json
      ├── README.md
      └── scripts/                 # Reproducibility
          ├── src/github_skills_dataset/
          ├── pyproject.toml
          └── README.md
```

## Output

The `build/` directory contains:
- **Data files**: files.parquet, repos.parquet, history.parquet
- **Metadata**: dataset-metadata.json, README.md
- **Scripts**: Complete source code for reproducibility

Upload the entire `build/` directory to Kaggle.
