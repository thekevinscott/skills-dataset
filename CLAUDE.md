# Skills Dataset

SKILL.md classifier pipeline for building a Kaggle dataset. See README.md for full documentation.

## Quick reference

- **Main pipeline** (no LLM): `filter-pass-1` (frontmatter) -> `filter-pass-2` (heuristics) -> `filter-pass-3` (SVM classifier)
- **Training data** (LLM, offline): `generate-training-data` sends uncertain files to Claude, results retrain the classifier
- **Export**: `export` writes Parquet for Kaggle
- Confidence scores in `validation_results.embedding_confidence` (0.0-1.0) let consumers choose quality threshold
- Labeled data: `data/labeled.csv` (generated from DB, not checked into git)
- DB: `data/validated.db` (SQLite with WAL mode)
