# GitHub Skills Dataset

Build a SKILL.md dataset from GitHub for Kaggle upload.

## Prerequisites

- An Anthropic API key (`ANTHROPIC_API_KEY` env var) for the `filter-valid-skills` command
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

Two-pass filter: rejects files without valid YAML frontmatter (free), then
classifies remaining files via an LLM. Results are cached on disk
(`~/.cache/skills-dataset/claude/`) so re-runs only pay for new files. Only
files with content on disk are processed; the rest are skipped until fetched.

Content is truncated to 3 KB for classification (frontmatter + intro is enough).

```bash
# Using Anthropic API (default model: claude-haiku-4-5-20251001)
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset filter-valid-skills \
  --main-db skills.db \
  --output-db validated.db \
  --content-dir content

# Using a local model via ollama
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset filter-valid-skills \
  --main-db skills.db \
  --output-db validated.db \
  --content-dir content \
  --base-url http://localhost:11434/v1 \
  --model qwen2.5:14b
```

Options: `--model`, `--base-url`

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
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset export --db validated.db --kaggle-username yourname
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
    filter/              # Two-pass validation (frontmatter + LLM)
      filter.py          # Main pipeline
      config.py          # Constants (model, cache dir, prompt)
      has_valid_frontmatter.py
      parse_github_url.py
      prompt_hash.py     # Cache key generation
      truncate_content.py
      validation_prompt.txt
    export.py            # Parquet export
    kaggle_metadata.py   # Kaggle dataset-metadata.json generation
  pyproject.toml
  README.md
```
