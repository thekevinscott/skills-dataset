"""Pass 4: Train embedding classifier and predict on all files."""

import csv
import hashlib
import random
import sqlite3
import time
from pathlib import Path

import numpy as np
from sklearn.metrics import accuracy_score, classification_report
from xgboost import XGBClassifier
from tqdm import tqdm

from .config import CONTENT_MAX_BYTES
from .embed import content_hash, blob_to_vector, DEFAULT_EMBEDDING_MODEL
from .filter import init_output_db, open_db, resolve_content_path
from .parse_github_url import parse_github_url


def load_labeled_csv(csv_path: Path) -> list[dict]:
    """Load labeled examples from CSV."""
    examples = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            examples.append({
                "url": row["url"],
                "is_skill": row["is_skill"].lower() == "true",
            })
    return examples


def get_embedding_vectors(db_path: Path, content_hashes: list[str], model: str) -> dict[str, np.ndarray]:
    """Look up embedding vectors from DB. Returns {content_hash: vector}."""
    conn = open_db(db_path)
    vectors = {}
    for ch in content_hashes:
        row = conn.execute(
            "SELECT vector FROM embeddings WHERE content_hash = ? AND model = ?",
            (ch, model)
        ).fetchone()
        if row:
            vectors[ch] = np.array(blob_to_vector(row[0]))
    conn.close()
    return vectors


async def classify_pass(args):
    """Train classifier on labeled data, predict on all files."""
    init_output_db(args.output_db)
    model = getattr(args, 'embedding_model', DEFAULT_EMBEDDING_MODEL)
    csv_path = Path(getattr(args, 'labeled_csv', 'data/labeled.csv'))
    threshold = getattr(args, 'confidence_threshold', None)

    # Load labeled data
    labeled = load_labeled_csv(csv_path)
    print(f"Labeled examples: {len(labeled):,}")

    # Resolve content hashes for labeled examples
    labeled_with_hash = []
    for ex in labeled:
        parsed = parse_github_url(ex["url"])
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        ch = content_hash(content)
        labeled_with_hash.append({**ex, "content_hash": ch})

    print(f"  With content on disk: {len(labeled_with_hash):,}")

    # Look up embeddings
    hashes = [ex["content_hash"] for ex in labeled_with_hash]
    vectors = get_embedding_vectors(args.output_db, hashes, model)
    labeled_with_vec = [ex for ex in labeled_with_hash if ex["content_hash"] in vectors]
    print(f"  With embeddings: {len(labeled_with_vec):,}")

    if len(labeled_with_vec) < 10:
        print("  Not enough labeled examples with embeddings. Run pass 3 first.")
        return

    # Stratified split: balanced val set, downsampled train
    random.seed(42)
    positives = [ex for ex in labeled_with_vec if ex["is_skill"]]
    negatives = [ex for ex in labeled_with_vec if not ex["is_skill"]]
    random.shuffle(positives)
    random.shuffle(negatives)

    # Hold out 1/3 of negatives for val, match with equal positives
    val_neg_count = max(len(negatives) // 3, 5)
    val = positives[:val_neg_count] + negatives[:val_neg_count]
    train_pos = positives[val_neg_count:]
    train_neg = negatives[val_neg_count:]

    # Downsample majority class to match minority for training
    train_down_pos = random.sample(train_pos, min(len(train_pos), len(train_neg)))
    train = train_down_pos + train_neg

    print(f"  Val (balanced): {len(val)} ({val_neg_count} pos, {val_neg_count} neg)")
    print(f"  Train (downsampled): {len(train)} ({len(train_down_pos)} pos, {len(train_neg)} neg)")

    # Train XGBoost
    X_train = np.array([vectors[ex["content_hash"]] for ex in train])
    y_train = np.array([1 if ex["is_skill"] else 0 for ex in train])
    X_val = np.array([vectors[ex["content_hash"]] for ex in val])
    y_val = np.array([1 if ex["is_skill"] else 0 for ex in val])

    clf = XGBClassifier(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        eval_metric='logloss', verbosity=0,
    )
    clf.fit(X_train, y_train)

    # Evaluate on val
    y_pred = clf.predict(X_val)
    y_prob = clf.predict_proba(X_val)[:, 1]
    print(f"\nVal accuracy: {accuracy_score(y_val, y_pred)*100:.1f}%")
    print(classification_report(y_val, y_pred, target_names=["reject", "skill"]))

    # Find confidence threshold for 95% accuracy
    if threshold is None:
        confidences = np.abs(y_prob - 0.5) * 2  # 0=uncertain, 1=confident
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
            threshold = 0.8
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

    # Resolve content hashes
    url_to_hash = {}
    for url in urls_to_classify:
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        url_to_hash[url] = content_hash(content)

    # Look up all embeddings
    all_hashes = list(set(url_to_hash.values()))
    all_vectors = get_embedding_vectors(args.output_db, all_hashes, model)

    # Predict
    conn = open_db(args.output_db)
    classified = 0
    no_embedding = 0

    for url, ch in tqdm(url_to_hash.items(), desc="Pass 4: classify", unit="url"):
        if ch not in all_vectors:
            no_embedding += 1
            continue
        vec = all_vectors[ch].reshape(1, -1)
        prob = clf.predict_proba(vec)[0, 1]
        is_skill = bool(prob >= 0.5)
        confidence = abs(prob - 0.5) * 2

        conn.execute(
            "UPDATE validation_results SET embedding_is_skill = ?, embedding_confidence = ? WHERE url = ?",
            (1 if is_skill else 0, round(confidence, 4), url)
        )
        classified += 1

        if classified % 5000 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    print(f"\n  Classified: {classified:,}")
    print(f"  Missing embeddings: {no_embedding:,}")
    print(f"  Confidence threshold for LLM fallback: {threshold}")

    # Report confidence distribution
    conn = open_db(args.output_db)
    above = conn.execute(
        "SELECT COUNT(*) FROM validation_results WHERE embedding_confidence >= ?",
        (threshold,)
    ).fetchone()[0]
    below = conn.execute(
        "SELECT COUNT(*) FROM validation_results WHERE embedding_confidence IS NOT NULL AND embedding_confidence < ?",
        (threshold,)
    ).fetchone()[0]
    conn.close()
    print(f"  Above threshold ({threshold}): {above:,} ({above/(above+below)*100:.0f}%)" if above + below > 0 else "")
    print(f"  Below threshold (need LLM): {below:,} ({below/(above+below)*100:.0f}%)" if above + below > 0 else "")
