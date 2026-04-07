# Skills Dataset

SKILL.md classifier pipeline for building a Kaggle dataset. See README.md for full documentation.

## Quick reference

- **Main pipeline** (no LLM): `filter-pass-1` (frontmatter) -> `filter-pass-2` (heuristics) -> `filter-pass-3` (SVM classifier)
- **Training data** (LLM, offline): `generate-training-data` sends uncertain files to Claude, appends to CSV
- **Export**: `export` writes Parquet for Kaggle
- Confidence scores in `validation_results.classifier_confidence` (0.0-1.0) let consumers choose quality threshold

## Architecture principles

- **CLI/Python 1:1 mapping**: Every CLI command must have a corresponding Python function. CLI is a thin wrapper. Logic never lives inline in CLI callbacks.
- **`training/labeled.csv` is the source of truth** for training data. Format: `content_hash,is_skill`. Checked into git. The DB and file cache are derived/transient.
- **`data/` is transient**: DBs, content files, builds. Can be deleted and regenerated. Never checked into git.
- **`training/` is persistent**: Labeled training data. Checked into git. Expensive to regenerate (LLM budget).
- **Content hashing**: `sha256(raw_bytes)` of the full file. No truncation. Truncation is a feature extraction detail, not an identity detail. Reproducible with `sha256sum`.
- **No LLM in main pipeline**: Passes 1-3 are free and local. LLM is offline training data generation only.
- **Cache is optional**: `~/.cache/skills-dataset/` can be blown away. Main pipeline doesn't use it. Training generator uses CSV for skip logic.
- **DB skip logic**: Only pass 1 uses the DB for skipping (frontmatter check). Everything else either re-runs (pass 2-3) or uses CSV (training generator).
- **Red-green TDD**: Write failing test first, then fix code. Never edit tests and production code simultaneously.
