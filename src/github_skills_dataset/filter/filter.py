"""Filter SKILL.md files: frontmatter check (pass 1) and DB schema management."""

import os
import sqlite3
import time
from pathlib import Path

from tqdm import tqdm

from .parse_github_url import parse_github_url
from .has_valid_frontmatter import has_valid_frontmatter


def resolve_content_path(content_dir: Path, owner: str, repo: str, ref: str, path: str) -> Path:
    """Build path to local content file."""
    return content_dir / owner / repo / "blob" / ref / path


def open_db(output_db: Path) -> sqlite3.Connection:
    """Open DB with WAL mode and busy timeout."""
    conn = sqlite3.connect(output_db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def init_output_db(output_db: Path):
    """Create/migrate the output database."""
    conn = open_db(output_db)

    # --- validation_results: pass 1 (frontmatter check) ---
    existing = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='validation_results'"
    ).fetchone()

    if existing:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(validation_results)")}
        if "is_skill" in cols:
            # Migrate: drop is_skill and reason, keep url + has_frontmatter
            conn.executescript("""
                CREATE TABLE validation_results_new (
                    url TEXT PRIMARY KEY,
                    has_frontmatter BOOLEAN NOT NULL,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                INSERT OR IGNORE INTO validation_results_new (url, has_frontmatter)
                    SELECT url, COALESCE(has_frontmatter, 0) FROM validation_results;
                DROP TABLE validation_results;
                ALTER TABLE validation_results_new RENAME TO validation_results;
            """)
    else:
        conn.execute("""
            CREATE TABLE validation_results (
                url TEXT PRIMARY KEY,
                has_frontmatter BOOLEAN NOT NULL,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    # --- validation_results: add pass 2-3 columns ---
    vr_cols = {row[1] for row in conn.execute("PRAGMA table_info(validation_results)")}

    # Migrate old column names
    if "embedding_is_skill" in vr_cols and "classifier_is_skill" not in vr_cols:
        conn.execute("ALTER TABLE validation_results RENAME COLUMN embedding_is_skill TO classifier_is_skill")
        conn.execute("ALTER TABLE validation_results RENAME COLUMN embedding_confidence TO classifier_confidence")
        vr_cols = {row[1] for row in conn.execute("PRAGMA table_info(validation_results)")}

    for col, typedef in [
        ("heuristic_reject", "BOOLEAN"),
        ("heuristic_reason", "TEXT"),
        ("classifier_is_skill", "BOOLEAN"),
        ("classifier_confidence", "REAL"),
    ]:
        if col not in vr_cols:
            conn.execute(f"ALTER TABLE validation_results ADD COLUMN {col} {typedef}")

    # --- embeddings: pass 3 (vector cache) ---
    conn.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            content_hash TEXT NOT NULL,
            model TEXT NOT NULL,
            vector BLOB NOT NULL,
            embedded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (content_hash, model)
        )
    """)

    # --- llm_skill_evaluation: pass 5 (LLM classification) ---
    eval_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_skill_evaluation'"
    ).fetchone()

    if eval_exists:
        eval_cols = {row[1] for row in conn.execute("PRAGMA table_info(llm_skill_evaluation)")}
        if "base_url" not in eval_cols:
            conn.execute("ALTER TABLE llm_skill_evaluation ADD COLUMN base_url TEXT")
        if "content_hash" not in eval_cols:
            conn.execute("ALTER TABLE llm_skill_evaluation ADD COLUMN content_hash TEXT")
    else:
        conn.execute("""
            CREATE TABLE llm_skill_evaluation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                content_hash TEXT,
                backend TEXT NOT NULL,
                model TEXT NOT NULL,
                base_url TEXT,
                is_skill BOOLEAN NOT NULL,
                reason TEXT,
                evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, backend, model)
            )
        """)

    conn.commit()
    conn.close()


def scan_content(args):
    """Scan source DB and content dir, return (all_urls, to_validate, no_content)."""
    main_conn = sqlite3.connect(args.main_db)
    all_urls = [row[0] for row in main_conn.execute("SELECT url FROM files").fetchall()]
    main_conn.close()

    t_start = time.monotonic()
    content_paths = set()
    for dirpath, _, filenames in os.walk(args.content_dir):
        for fname in filenames:
            content_paths.add(os.path.join(dirpath, fname))

    to_validate = []
    no_content = 0
    for url in all_urls:
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        if str(resolve_content_path(args.content_dir, owner, repo, ref, path)) in content_paths:
            to_validate.append(url)
        else:
            no_content += 1

    t_scan = time.monotonic() - t_start
    print(f"URLs in DB: {len(all_urls):,} (scan: {t_scan:.1f}s)")
    print(f"  Content on disk: {len(to_validate):,}")
    print(f"  Not yet fetched:  {no_content:,}")

    return all_urls, to_validate, no_content


async def filter_pass1(args, to_validate=None):
    """Pass 1: check frontmatter and persist results to DB.

    Returns to_validate for chaining (avoids re-scanning in filter()).
    """
    init_output_db(args.output_db)
    if to_validate is None:
        _, to_validate, _ = scan_content(args)

    out_conn = open_db(args.output_db)
    already_checked = set(
        row[0] for row in out_conn.execute("SELECT url FROM validation_results").fetchall()
    )
    out_conn.close()

    unchecked = [url for url in to_validate if url not in already_checked]
    skipped = len(to_validate) - len(unchecked)
    print(f"  Already checked: {skipped:,}, to check: {len(unchecked):,}")

    frontmatter_results = []
    t_start = time.monotonic()
    for url in tqdm(unchecked, desc="Pass 1: frontmatter", unit="file"):
        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        content = local_path.read_text(errors='replace')
        frontmatter_results.append((url, has_valid_frontmatter(content)))

    no_frontmatter = sum(1 for _, fm in frontmatter_results if not fm)
    yes_frontmatter = sum(1 for _, fm in frontmatter_results if fm)

    t_pass1 = time.monotonic() - t_start
    print(f"\nPass 1 - frontmatter check ({t_pass1:.1f}s):")
    print(f"  No valid frontmatter: {no_frontmatter:,}")
    print(f"  Has frontmatter: {yes_frontmatter:,}")

    out_conn = open_db(args.output_db)
    for i, (url, has_fm) in enumerate(frontmatter_results):
        out_conn.execute(
            "INSERT OR IGNORE INTO validation_results (url, has_frontmatter) VALUES (?, ?)",
            (url, 1 if has_fm else 0)
        )
        if (i + 1) % 1000 == 0:
            out_conn.commit()
    out_conn.commit()
    out_conn.close()

    return to_validate
