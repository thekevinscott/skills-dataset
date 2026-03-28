# GitHub Skills Dataset

Build a SKILL.md dataset from GitHub for Kaggle upload.

## Prerequisites

- An Anthropic API key (`ANTHROPIC_API_KEY` env var) for the `filter-valid-skills` command (or `claude-agent-sdk` for subscription billing)
- A GitHub token in `.env` (`GITHUB_TOKEN=...`)

## Installation

```bash
uv sync
```

## Pipeline

### 1. Fetch file paths

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-paths "filename:SKILL.md" --db data/skills.db
```

### 2. Fetch file content

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-content --db data/skills.db --content-dir data/content --graphql
```

The `--graphql` flag batches 50 files per query and is ~50x faster than the
default REST path. Omit it to fall back to REST (10 concurrent threads, ~1.3
req/s throttled).

### 3. Filter valid skills

Two-pass filter: rejects files without valid YAML frontmatter (free), then
classifies remaining files via an LLM. Results are cached on disk
(`~/.cache/skills-dataset/claude/`) so re-runs only pay for new files. Only
files with content on disk are processed; the rest are skipped until fetched.

Content is truncated to 3 KB for classification (frontmatter + intro is enough).

```bash
# Using Claude Agent SDK (subscription billing, no per-token cost)
uvx --from 'github-skills-dataset[agent] @ git+https://github.com/thekevinscott/skills-dataset' \
  skills-dataset filter-valid-skills \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content \
  --backend claude-agent-sdk \
  --concurrency 3

# Using Anthropic API (per-token billing, default model: claude-haiku-4-5-20251001)
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset filter-valid-skills \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content

# Using a local model via ollama
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset filter-valid-skills \
  --main-db data/skills.db \
  --output-db data/validated.db \
  --content-dir data/content \
  --base-url http://localhost:11434/v1 \
  --model qwen2.5:14b
```

Options: `--model`, `--base-url`, `--backend`, `--concurrency`

### 4. Fetch metadata and history

Run against the filtered DB so we only fetch data for valid skills.

```bash
uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-repo-metadata --db data/validated.db

uvx --from git+https://github.com/thekevinscott/github-data-file-fetcher \
  github-fetch fetch-file-history --db data/validated.db
```

### 5. Export to Parquet

```bash
uvx --from git+https://github.com/thekevinscott/skills-dataset \
  skills-dataset export --db data/validated.db --kaggle-username yourname
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
