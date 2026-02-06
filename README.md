# GitHub Skills Dataset

Build a SKILL.md dataset from GitHub for Kaggle upload.

## Prerequisites

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) (the `filter-valid-skills` command uses the Claude Agent SDK)
- A GitHub token in `.env` (`GITHUB_TOKEN=...`)

## Installation

```bash
uv sync
```

## Pipeline

### 1. Fetch file paths

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-paths "filename:SKILL.md" --db skills.db
```

### 2. Fetch file content

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-content --db skills.db --content-dir content
```

### 3. Filter valid skills

Uses Claude to classify each file. Produces a filtered DB with only valid skills.

```bash
uv run skills-dataset filter-valid-skills \
  --main-db skills.db \
  --output-db validated.db \
  --content-dir content
```

Options: `--batch-size`, `--max-concurrent`, `--model`

### 4. Fetch metadata and history

Run against the filtered DB so we only fetch data for valid skills.

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-repo-metadata --db validated.db

uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-history --db validated.db
```

### 5. Export to Parquet

```bash
uv run skills-dataset export --db validated.db --kaggle-username yourname
```

### 6. Upload to Kaggle

```bash
cd build && kaggle datasets create -p . --dir-mode tar
```

## Project Structure

```
skills-dataset/
  src/github_skills_dataset/
    cli.py               # Click CLI
    filter.py            # Two-pass validation (frontmatter + Claude)
    export.py            # Parquet export
    kaggle_metadata.py   # Kaggle dataset-metadata.json generation
  pyproject.toml
  README.md
  build/                 # Export output (gitignored)
    files.parquet
    repos.parquet
    history.parquet
    scripts/             # Source code for reproducibility
```
