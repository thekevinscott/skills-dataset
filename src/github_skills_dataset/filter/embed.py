"""Pass 3: Generate embeddings for SKILL.md files via Ollama."""

import hashlib
import sqlite3
import struct
import time
from pathlib import Path

import httpx
from tqdm import tqdm

from .config import CONTENT_MAX_BYTES
from .filter import init_output_db, open_db, scan_content, resolve_content_path
from .parse_github_url import parse_github_url

DEFAULT_EMBEDDING_MODEL = "nomic-embed-text"
DEFAULT_OLLAMA_URL = "http://localhost:11434"


def content_hash(content: str) -> str:
    """SHA256 hash of truncated content."""
    return hashlib.sha256(content[:CONTENT_MAX_BYTES].encode()).hexdigest()


def vector_to_blob(vector: list[float]) -> bytes:
    """Pack float list to bytes."""
    return struct.pack(f'{len(vector)}f', *vector)


def blob_to_vector(blob: bytes) -> list[float]:
    """Unpack bytes to float list."""
    n = len(blob) // 4
    return list(struct.unpack(f'{n}f', blob))


async def embed_pass(args):
    """Embed all files with frontmatter, skipping already-cached content hashes."""
    init_output_db(args.output_db)
    model = getattr(args, 'embedding_model', DEFAULT_EMBEDDING_MODEL)
    ollama_url = getattr(args, 'ollama_url', DEFAULT_OLLAMA_URL)

    _, to_validate, _ = scan_content(args)

    # Get URLs with frontmatter
    conn = open_db(args.output_db)
    has_fm = set(
        row[0] for row in conn.execute(
            "SELECT url FROM validation_results WHERE has_frontmatter = 1"
        ).fetchall()
    )
    conn.close()

    urls_to_embed = [url for url in to_validate if url in has_fm]
    print(f"  URLs with frontmatter: {len(urls_to_embed):,}")

    # Compute content hashes and find which are missing from DB
    url_hashes = {}  # url -> (content_hash, content)
    for url in urls_to_embed:
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        ch = content_hash(content)
        url_hashes[url] = (ch, content[:CONTENT_MAX_BYTES])

    all_hashes = set(ch for ch, _ in url_hashes.values())

    conn = open_db(args.output_db)
    existing = set(
        row[0] for row in conn.execute(
            "SELECT content_hash FROM embeddings WHERE model = ?", (model,)
        ).fetchall()
    )
    conn.close()

    missing = all_hashes - existing
    print(f"  Unique content hashes: {len(all_hashes):,}")
    print(f"  Already embedded: {len(existing):,}")
    print(f"  To embed: {len(missing):,}")

    if not missing:
        print("  Nothing to embed.")
        return

    # Build list of (hash, content) to embed
    hash_to_content = {}
    for ch, content in url_hashes.values():
        if ch in missing and ch not in hash_to_content:
            hash_to_content[ch] = content

    # Embed in batches via Ollama
    batch_size = 32
    items = list(hash_to_content.items())
    conn = open_db(args.output_db)

    t_start = time.monotonic()
    async with httpx.AsyncClient(timeout=120.0) as client:
        for i in tqdm(range(0, len(items), batch_size), desc="Pass 3: embed", unit="batch"):
            batch = items[i:i + batch_size]
            texts = [content for _, content in batch]

            resp = await client.post(
                f"{ollama_url}/api/embed",
                json={"model": model, "input": texts},
            )
            resp.raise_for_status()
            data = resp.json()
            vectors = data["embeddings"]

            for (ch, _), vec in zip(batch, vectors):
                conn.execute(
                    "INSERT OR IGNORE INTO embeddings (content_hash, model, vector) VALUES (?, ?, ?)",
                    (ch, model, vector_to_blob(vec))
                )

            if (i // batch_size + 1) % 10 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    t_elapsed = time.monotonic() - t_start
    print(f"  Embedded {len(items):,} items in {t_elapsed:.1f}s")
