"""Pass 3: Train SVM classifier on TF-IDF + heuristic features + frontmatter BoW."""

import csv
import random
import re
import sqlite3
from pathlib import Path

import numpy as np
import yaml
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from tqdm import tqdm

from .config import CONTENT_MAX_BYTES
from .filter import init_output_db, open_db, resolve_content_path
from .parse_github_url import parse_github_url


def load_labeled_csv(csv_path: Path) -> list[dict]:
    """Load labeled examples from CSV."""
    examples = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            examples.append({
                "url": row["url"],
                "is_skill": row["is_skill"].lower() == "true",
            })
    return examples


def _parse_frontmatter(content: str) -> dict:
    match = re.match(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}
    try:
        result = yaml.safe_load(match.group(1))
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def _get_body(content: str) -> str:
    match = re.match(r'^---\s*\n.*?\n---\s*\n?(.*)', content, re.DOTALL)
    return match.group(1) if match else content


def extract_heuristic_features(content: str, url: str) -> list[float]:
    """Extract structural/heuristic features as a numeric vector."""
    fm = _parse_frontmatter(content)
    body = _get_body(content).strip()
    fm_str = str(fm).lower()
    url_lower = url.lower()

    return [
        # Frontmatter structure
        1 if 'name' in fm else 0,
        1 if 'description' in fm else 0,
        1 if 'title' in fm and 'name' not in fm else 0,
        1 if 'title' in fm and 'name' in fm else 0,
        1 if re.search(r'date', fm_str) else 0,
        1 if any(k in fm for k in ['categories', 'tags', 'layout', 'author', 'authors']) else 0,
        1 if 'arxiv' in fm_str else 0,
        1 if 'skillxiv' in fm_str else 0,
        1 if all(k in fm for k in ['about', 'labels', 'assignees']) else 0,
        1 if any(k in fm for k in ['price', 'revenue_potential']) else 0,
        1 if '<agent-activation' in content.lower() else 0,
        1 if 'platform' in fm and 'claude' not in str(fm.get('platform', '')).lower() else 0,
        1 if all(k in fm for k in ['emoji', 'github_url', 'triggers']) else 0,
        1 if 'allowed-tools' in fm_str or 'allowed_tools' in fm_str else 0,
        1 if 'version' in fm else 0,
        1 if 'license' in fm else 0,
        1 if 'metadata' in fm else 0,
        1 if 'autoload' in fm else 0,

        # Body structure
        len(body),
        min(len(body), 5000),
        body.count('## '),
        body.count('### '),
        body.count('```'),
        body.count('- '),
        body.count('|'),
        body.count('\n'),
        1 if re.search(r'(?i)(step \d|workflow|## how to|## usage|## when to)', body) else 0,
        1 if re.search(r'(?i)(claude|claude code)', body) else 0,
        1 if re.search(r'(?i)(when to use|use this skill|trigger)', body) else 0,
        1 if re.search(r'(?i)(## constraints|## rules|## principles)', body) else 0,
        1 if re.search(r'(?i)(## output|## input|## example)', body) else 0,
        len(fm),

        # Ratios
        body.count('## ') / max(body.count('\n'), 1),
        body.count('```') / max(body.count('\n'), 1),
        body.count('- ') / max(body.count('\n'), 1),
        len(body) / max(len(content), 1),

        # URL features
        1 if '.claude/skills' in url_lower else 0,
        1 if '.agents/skills' in url_lower else 0,
        1 if 'blog' in url_lower else 0,
        1 if '_posts' in url_lower else 0,
        1 if 'docs/' in url_lower else 0,
        1 if 'test' in url_lower else 0,
        1 if '/skills/' in url_lower else 0,
        url_lower.count('/'),

        # Content patterns
        1 if 'CRITICAL' in content else 0,
        1 if re.search(r'(?i)you (are|should|must|will)', body[:500]) else 0,
        1 if re.search(r'(?i)(api|sdk|library|package|pip install|npm install)', body) else 0,
        1 if re.search(r'(?i)(研究|论文|paper|abstract|methodology)', body) else 0,
        1 if re.search(r'(?i)(blog|posted|published|written by)', fm_str) else 0,
        content.count('http://') + content.count('https://'),
        len(re.findall(r'`[^`]+`', body)),
    ]


def extract_url_features(url: str) -> list[float]:
    """Extract features from the URL itself."""
    u = url.lower()
    return [
        1 if '.claude/skills' in u else 0,
        1 if '.agents/skills' in u else 0,
        1 if 'blog' in u else 0,
        1 if '_posts' in u else 0,
        1 if 'docs/' in u else 0,
        1 if 'test' in u else 0,
        1 if '/skills/' in u else 0,
        u.count('/'),
    ]


def _build_fm_bow_vocab(examples_content: list[str]) -> list[str]:
    """Build vocabulary of frontmatter keys across all examples."""
    keys = set()
    for content in examples_content:
        fm = _parse_frontmatter(content)
        if isinstance(fm, dict):
            keys.update(fm.keys())
    return sorted(keys)


def _fm_bow_vector(content: str, vocab: list[str]) -> np.ndarray:
    """Encode frontmatter keys as bag-of-words."""
    fm = _parse_frontmatter(content)
    idx = {k: i for i, k in enumerate(vocab)}
    vec = np.zeros(len(vocab))
    if isinstance(fm, dict):
        for k in fm:
            if k in idx:
                vec[idx[k]] = 1
    return vec


async def classify_pass(args):
    """Train SVM classifier on labeled data, predict on all files."""
    init_output_db(args.output_db)
    csv_path = Path(getattr(args, 'labeled_csv', 'data/labeled.csv'))
    threshold = getattr(args, 'confidence_threshold', None)

    # Load labeled data with content
    labeled = load_labeled_csv(csv_path)
    print(f"Labeled examples: {len(labeled):,}")

    labeled_with_content = []
    for ex in labeled:
        parsed = parse_github_url(ex["url"])
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        labeled_with_content.append({**ex, "content": content})

    print(f"  With content on disk: {len(labeled_with_content):,}")

    if len(labeled_with_content) < 10:
        print("  Not enough labeled examples. Need at least 10.")
        return

    # Stratified split: balanced val set, downsampled train
    random.seed(42)
    positives = [ex for ex in labeled_with_content if ex["is_skill"]]
    negatives = [ex for ex in labeled_with_content if not ex["is_skill"]]
    random.shuffle(positives)
    random.shuffle(negatives)

    val_neg_count = max(len(negatives) // 3, 5)
    val = positives[:val_neg_count] + negatives[:val_neg_count]
    train_pos = positives[val_neg_count:]
    train_neg = negatives[val_neg_count:]
    train_down_pos = random.sample(train_pos, min(len(train_pos), len(train_neg)))
    train = train_down_pos + train_neg

    print(f"  Val (balanced): {len(val)} ({val_neg_count} pos, {val_neg_count} neg)")
    print(f"  Train (downsampled): {len(train)} ({len(train_down_pos)} pos, {len(train_neg)} neg)")

    # Build features
    all_content = [ex["content"] for ex in labeled_with_content]
    fm_vocab = _build_fm_bow_vocab(all_content)

    tfidf = TfidfVectorizer(max_features=1000, ngram_range=(1, 2), sublinear_tf=True)
    X_train_tfidf = tfidf.fit_transform([ex["content"][:CONTENT_MAX_BYTES] for ex in train]).toarray()
    X_val_tfidf = tfidf.transform([ex["content"][:CONTENT_MAX_BYTES] for ex in val]).toarray()

    X_train_heur = np.array([extract_heuristic_features(ex["content"], ex["url"]) for ex in train])
    X_val_heur = np.array([extract_heuristic_features(ex["content"], ex["url"]) for ex in val])

    X_train_url = np.array([extract_url_features(ex["url"]) for ex in train])
    X_val_url = np.array([extract_url_features(ex["url"]) for ex in val])

    X_train_fmbow = np.array([_fm_bow_vector(ex["content"], fm_vocab) for ex in train])
    X_val_fmbow = np.array([_fm_bow_vector(ex["content"], fm_vocab) for ex in val])

    X_train = np.hstack([X_train_tfidf, X_train_heur, X_train_url, X_train_fmbow])
    X_val = np.hstack([X_val_tfidf, X_val_heur, X_val_url, X_val_fmbow])
    y_train = np.array([1 if ex["is_skill"] else 0 for ex in train])
    y_val = np.array([1 if ex["is_skill"] else 0 for ex in val])

    print(f"  Features: {X_train.shape[1]} (TF-IDF={X_train_tfidf.shape[1]}, heuristic={X_train_heur.shape[1]}, URL={X_train_url.shape[1]}, FM BoW={X_train_fmbow.shape[1]})")

    # Train SVM-rbf
    clf = Pipeline([
        ('scaler', StandardScaler()),
        ('svm', SVC(kernel='rbf', C=10, probability=True, class_weight='balanced')),
    ])
    clf.fit(X_train, y_train)

    # Evaluate on val
    y_pred = clf.predict(X_val)
    y_prob = clf.predict_proba(X_val)[:, 1]
    f1_r = f1_score(y_val, y_pred, pos_label=0)
    f1_s = f1_score(y_val, y_pred, pos_label=1)
    print(f"\nVal accuracy: {accuracy_score(y_val, y_pred)*100:.1f}%, macro-F1: {(f1_r+f1_s)/2:.3f}")
    print(classification_report(y_val, y_pred, target_names=["reject", "skill"]))

    # Find confidence threshold
    if threshold is None:
        confidences = np.abs(y_prob - 0.5) * 2
        for t in np.arange(0.0, 1.0, 0.05):
            mask = confidences >= t
            if mask.sum() < 5:
                continue
            acc = accuracy_score(y_val[mask], y_pred[mask])
            coverage = mask.sum() / len(y_val)
            if acc >= 0.95:
                threshold = t
                print(f"  Auto threshold: {t:.2f} (accuracy={acc*100:.1f}%, coverage={coverage*100:.0f}%)")
                break
        if threshold is None:
            threshold = 0.5
            print(f"  Could not find 95% threshold, using default: {threshold}")

    # Predict on all URLs with frontmatter and no heuristic reject
    conn = open_db(args.output_db)
    urls_to_classify = [
        row[0] for row in conn.execute(
            "SELECT url FROM validation_results WHERE has_frontmatter = 1 AND (heuristic_reject IS NULL OR heuristic_reject != 1)"
        ).fetchall()
    ]
    conn.close()
    print(f"\nPredicting on {len(urls_to_classify):,} URLs...")

    # Build features for all URLs
    url_features = []  # (url, content_truncated, heuristic_vec, url_vec, fmbow_vec)
    for url in tqdm(urls_to_classify, desc="Pass 3: extract features", unit="url"):
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        url_features.append((
            url,
            content[:CONTENT_MAX_BYTES],
            extract_heuristic_features(content, url),
            extract_url_features(url),
            _fm_bow_vector(content, fm_vocab),
        ))

    # TF-IDF transform
    X_tfidf = tfidf.transform([uf[1] for uf in url_features]).toarray()
    X_heur = np.array([uf[2] for uf in url_features])
    X_url = np.array([uf[3] for uf in url_features])
    X_fmbow = np.array([uf[4] for uf in url_features])
    X_all = np.hstack([X_tfidf, X_heur, X_url, X_fmbow])

    # Predict
    probs = clf.predict_proba(X_all)[:, 1]
    preds = (probs >= 0.5).astype(int)
    confidences = np.abs(probs - 0.5) * 2

    # Write results
    conn = open_db(args.output_db)
    for i, (url, _, _, _, _) in enumerate(tqdm(url_features, desc="Pass 3: write results", unit="url")):
        conn.execute(
            "UPDATE validation_results SET embedding_is_skill = ?, embedding_confidence = ? WHERE url = ?",
            (int(preds[i]), round(float(confidences[i]), 4), url)
        )
        if (i + 1) % 5000 == 0:
            conn.commit()
    conn.commit()
    conn.close()

    # Report
    above = int((confidences >= threshold).sum())
    below = int((confidences < threshold).sum())
    total = above + below
    print(f"\n  Classified: {total:,}")
    print(f"  Confidence threshold: {threshold}")
    print(f"  Above threshold: {above:,} ({above/total*100:.0f}%)")
    print(f"  Below threshold (need LLM): {below:,} ({below/total*100:.0f}%)")
