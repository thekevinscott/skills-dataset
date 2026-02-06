"""Filter valid SKILL.md files using Claude via the Message Batches API (50% discount)."""

import asyncio
import json
import os
import re
import sqlite3
from pathlib import Path

import anthropic
from .config import CACHE_DIR, DEFAULT_MODEL, BATCH_CHUNK_SIZE, VALIDATION_PROMPT
from .parse_github_url import parse_github_url
from .has_valid_frontmatter import has_valid_frontmatter
from .prompt_hash import prompt_hash
from .truncate_content import truncate_content


def get_cached_result(content_hash: str) -> dict | None:
    """Check file cache for a previous result."""
    cache_file = CACHE_DIR / f"{content_hash}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    return None


def insert_cached_result(content_hash: str, is_skill: bool, reason: str):
    """Store result in file cache."""
    cache_file = CACHE_DIR / f"{content_hash}.json"
    cache_file.write_text(json.dumps({"is_skill": is_skill, "reason": reason}))


def parse_response(text: str) -> dict:
    """Parse JSON from Claude response with fallbacks."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    match = re.search(r'\{.*"is_skill".*\}', text, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    raise ValueError(f"Could not parse JSON: {text[:200]}")




def resolve_content_path(content_dir: Path, owner: str, repo: str, ref: str, path: str) -> Path:
    """Build path to local content file."""
    return content_dir / owner / repo / "blob" / ref / path


def init_output_db(output_db: Path):
    """Create the output database with validation_results and files tables."""
    conn = sqlite3.connect(output_db)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS validation_results (
            url TEXT PRIMARY KEY,
            is_skill BOOLEAN NOT NULL,
            reason TEXT,
            validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS files (
            url TEXT PRIMARY KEY,
            sha TEXT,
            size_bytes INTEGER,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()


def rebuild_files_table(main_db: Path, output_db: Path):
    """Copy valid file rows from main DB into output DB's files table."""
    out = sqlite3.connect(output_db)
    out.execute("DELETE FROM files")

    main = sqlite3.connect(main_db)
    valid_urls = {row[0] for row in out.execute(
        "SELECT url FROM validation_results WHERE is_skill = 1"
    ).fetchall()}

    rows = main.execute("SELECT url, sha, size_bytes, discovered_at FROM files").fetchall()
    inserted = 0
    for row in rows:
        if row[0] in valid_urls:
            out.execute("INSERT OR IGNORE INTO files VALUES (?,?,?,?)", row)
            inserted += 1

    out.commit()
    main.close()
    out.close()
    return inserted


async def filter(args):
    """Main filter pipeline using Anthropic Message Batches API (50% discount)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    init_output_db(args.output_db)
    model = args.model or DEFAULT_MODEL

    # Get all URLs from source DB
    main_conn = sqlite3.connect(args.main_db)
    all_urls = [row[0] for row in main_conn.execute("SELECT url FROM files").fetchall()]
    main_conn.close()

    # Content on disk
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

    print(f"Total: {len(all_urls):,}, Content available: {len(to_validate):,}, No content yet: {no_content:,}")
    local_results = []
    uncached = {}            # cache_key -> (truncated_content, [urls])
    for url in to_validate:
        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        content = local_path.read_text(errors='replace')
        if has_valid_frontmatter(content):
            cache_key = prompt_hash(content)

            cached = get_cached_result(cache_key)
            if cached is not None:
                local_results.append((url, cached["is_skill"], cached.get("reason", "")))
            else:
                truncated = truncate_content(content)

                # Deduplicate by content -- same content across repos only needs one API call
                if cache_key in uncached:
                    uncached[cache_key][1].append(url)
                else:
                    uncached[cache_key] = (truncated, [url])

    total_uncached = 0
    for k in uncached.keys():
        _, urls = uncached[k]
        total_uncached += len(urls)
    print(f'Cached: {len(local_results)}, uncached: {total_uncached}')

    # Write cached/frontmatter-rejected results to DB
    out_conn = sqlite3.connect(args.output_db)
    for url, is_skill, reason in local_results:
        out_conn.execute(
            "INSERT OR REPLACE INTO validation_results (url, is_skill, reason) VALUES (?, ?, ?)",
            (url, is_skill, reason)
        )
    out_conn.commit()
    out_conn.close()

    if not uncached:
        final_valid = rebuild_files_table(args.main_db, args.output_db)
        print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
        return

    # --- Phase 2: Submit to Batches API ---
    try:
        client = anthropic.AsyncAnthropic()
        unique_items = list(uncached.items())  # [(cache_key, (content, [urls]))]

        for chunk_start in range(0, len(unique_items), BATCH_CHUNK_SIZE):
            chunk = unique_items[chunk_start:chunk_start + BATCH_CHUNK_SIZE]

            requests = []
            for cache_key, (content, _urls) in chunk:
                requests.append({
                    "custom_id": cache_key,
                    "params": {
                        "model": model,
                        "max_tokens": 256,
                        "messages": [{"role": "user", "content": VALIDATION_PROMPT.format(content=content)}],
                    }
                })

            batch = await client.messages.batches.create(requests=requests)
            print(f"\nSubmitted batch {batch.id} ({len(requests):,} requests)")

            # Poll for completion
            while batch.processing_status != "ended":
                await asyncio.sleep(30)
                batch = await client.messages.batches.retrieve(batch.id)
                done = (batch.request_counts.succeeded
                        + batch.request_counts.errored
                        + batch.request_counts.expired)
                print(f"  Progress: {done:,}/{len(requests):,} "
                      f"(succeeded: {batch.request_counts.succeeded:,}, "
                      f"errored: {batch.request_counts.errored:,})")

            # --- Phase 3: Process results ---
            out_conn = sqlite3.connect(args.output_db)
            valid_count = 0
            invalid_count = 0
            error_count = 0

            result_stream = await client.messages.batches.results(batch.id)
            async for result in result_stream:
                cache_key = result.custom_id
                _, urls = uncached[cache_key]

                if result.result.type == "succeeded":
                    try:
                        text = result.result.message.content[0].text
                        parsed = parse_response(text)
                        is_skill = parsed.get("is_skill", False)
                        reason = parsed.get("reason", "")
                    except Exception as e:
                        is_skill = False
                        reason = f"Parse error: {str(e)[:50]}"
                        error_count += 1
                else:
                    is_skill = False
                    reason = f"API error: {result.result.type}"
                    error_count += 1

                insert_cached_result(cache_key, is_skill, reason)

                for url in urls:
                    out_conn.execute(
                        "INSERT OR REPLACE INTO validation_results (url, is_skill, reason) VALUES (?, ?, ?)",
                        (url, is_skill, reason)
                    )
                    if is_skill:
                        valid_count += 1
                    else:
                        invalid_count += 1

            out_conn.commit()
            out_conn.close()
            print(f"  Results: {valid_count:,} valid, {invalid_count:,} rejected, {error_count:,} errors")
    except Exception as e:
        print(f"\nBatch API error: {e}")

    # --- Phase 4: Rebuild files table (always runs) ---
    final_valid = rebuild_files_table(args.main_db, args.output_db)
    print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
