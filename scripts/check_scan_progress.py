"""Read-only progress check on the running fetch-file-paths scan."""
import sqlite3

DB = "/home/duncan/work/code/research/skills-analysis/skills-dataset/data/skills.db"

conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=5)
row = conn.execute(
    "SELECT last_lo, max_size, collected, completed_at, updated_at "
    "FROM scan_progress WHERE query = 'filename:SKILL.md'"
).fetchone()
n_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
conn.close()

if row is None:
    print("no scan_progress row found")
else:
    last_lo, max_size, collected, completed_at, updated_at = row
    pct = 100.0 * last_lo / max_size if max_size else 0
    print(f"last_lo={last_lo:,} / {max_size:,} bytes ({pct:.1f}% of size axis)")
    print(f"collected={collected:,}  files_table={n_files:,}")
    print(f"completed_at={completed_at}  updated_at={updated_at} (UTC)")
