# Classification passes

## Pass 1: Frontmatter check

Rejects files without valid YAML frontmatter. Results cached in DB -- re-runs skip already-checked URLs. ~50s on warm run.

## Pass 2: Heuristic rejection

Deterministic rules catch definite non-skills. Re-runs every time (~6 min for 965K files). Sets `heuristic_reject=1` for rejects.

Rules:
- Prompt injection patterns (`<agent-activation>`)
- Academic papers (skillXiv engine, arxiv URLs)
- Blog posts (title + date + categories, no name field)
- GitHub issue templates (about + labels + assignees)
- Commercial content (price/revenue fields)
- Non-Claude platforms (platform field not containing "claude")
- Empty body (< 50 chars after frontmatter)
- Tool documentation cards (emoji + github_url + triggers, no name)

## Pass 3: SVM classifier

Trains an SVM-rbf classifier on labeled data, predicts `is_skill` for all files. Processes in 10K batches (~2GB peak memory). Retrains from scratch every run.

Features: TF-IDF bigrams (1000) + heuristic features (51) + URL features (8) + frontmatter key bag-of-words.
