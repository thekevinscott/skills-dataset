"""Filter valid SKILL.md files using an LLM API."""

import asyncio
import json
import os
import re
import sqlite3
from pathlib import Path

import anthropic
import httpx
from .config import CACHE_DIR, DEFAULT_MODEL, VALIDATION_PROMPT

DEFAULT_CONCURRENCY = 10
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
    uncached = {}            # cache_key -> (content, [urls])
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
                # Deduplicate by content -- same content across repos only needs one API call
                if cache_key in uncached:
                    uncached[cache_key][1].append(url)
                else:
                    uncached[cache_key] = (content, [url])

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

    # --- Phase 2: Concurrent API calls ---
    base_url = getattr(args, 'base_url', None)
    concurrency = getattr(args, 'concurrency', DEFAULT_CONCURRENCY)
    client_kwargs = {}
    if base_url:
        client_kwargs["base_url"] = base_url
        # Dummy key required by SDK even for local endpoints
        client_kwargs["api_key"] = "sk-ant-dummy-key-for-local-endpoint"
    client = anthropic.AsyncAnthropic(**client_kwargs)
    semaphore = asyncio.Semaphore(concurrency)

    async def validate_one(cache_key, content):
        async with semaphore:
            prompt = VALIDATION_PROMPT.format(content=content)
            message = await client.messages.create(
                model=model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            text = message.content[0].text
            return parse_response(text)

    unique_items = list(uncached.items())
    out_conn = sqlite3.connect(args.output_db)
    valid_count = 0
    invalid_count = 0
    error_count = 0
    first_error = None
    completed = 0
    total = len(unique_items)

    async def process_one(cache_key, content, urls):
        """Validate and return result with metadata."""
        try:
            result = await validate_one(cache_key, content)
            is_skill = result.get("is_skill", False)
            reason = result.get("reason", "")
            return cache_key, urls, is_skill, reason, None
        except Exception as e:
            return cache_key, urls, False, f"Error: {str(e)[:80]}", e

    # Launch all tasks - semaphore controls concurrency
    tasks = [
        asyncio.create_task(process_one(cache_key, content, urls))
        for cache_key, (content, urls) in unique_items
    ]

    print(f"\nProcessing {total:,} unique files (concurrency: {concurrency})...")

    for coro in asyncio.as_completed(tasks):
        cache_key, urls, is_skill, reason, error = await coro
        completed += 1

        if error and first_error is None:
            first_error = error
            print(f"\nFirst error: {first_error}")

        if error:
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

        # Running progress on same line
        print(f"\r{completed:,}/{total:,} - valid: {valid_count:,}, rejected: {invalid_count:,}, errors: {error_count:,}  ", end="", flush=True)

        # Commit every 100
        if completed % 100 == 0:
            out_conn.commit()

    out_conn.commit()
    out_conn.close()
    print(f"\nDone: {completed:,}/{total:,} - valid: {valid_count:,}, rejected: {invalid_count:,}, errors: {error_count:,}")

    # --- Phase 4: Rebuild files table (always runs) ---
    final_valid = rebuild_files_table(args.main_db, args.output_db)
    print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
