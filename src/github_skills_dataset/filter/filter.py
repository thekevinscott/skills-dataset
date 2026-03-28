"""Filter valid SKILL.md files using an LLM API."""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path

from cachetta import async_read_cache, async_write_cache
from tqdm import tqdm
from .config import DEFAULT_MODEL, VALIDATION_PROMPT, llm_cache

DEFAULT_CONCURRENCY = 10
from .parse_github_url import parse_github_url
from .has_valid_frontmatter import has_valid_frontmatter
from .truncate_content import truncate_content


def make_cache_key(prompt: str, model: str, base_url: str | None) -> str:
    """Hash prompt + model + base_url into a cache filename."""
    key_data = json.dumps({"prompt": prompt, "model": model, "base_url": base_url}, sort_keys=True)
    return hashlib.sha256(key_data.encode()).hexdigest()


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
    """Create the output database with validation_results table."""
    conn = sqlite3.connect(output_db)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS validation_results (
            url TEXT PRIMARY KEY,
            has_frontmatter BOOLEAN NOT NULL DEFAULT 1,
            is_skill BOOLEAN NOT NULL,
            reason TEXT,
            validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Migration: add has_frontmatter to existing DBs
    try:
        conn.execute("ALTER TABLE validation_results ADD COLUMN has_frontmatter BOOLEAN NOT NULL DEFAULT 1")
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.commit()
    conn.close()



async def filter(args):
    """Main filter pipeline using Anthropic Message Batches API (50% discount)."""
    init_output_db(args.output_db)
    model = args.model or DEFAULT_MODEL
    base_url = getattr(args, 'base_url', None)

    # Get all URLs from source DB
    main_conn = sqlite3.connect(args.main_db)
    all_urls = [row[0] for row in main_conn.execute("SELECT url FROM files").fetchall()]
    main_conn.close()

    # Content on disk
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

    # Load existing results from output DB (skip URLs already processed without errors)
    out_conn = sqlite3.connect(args.output_db)
    existing_results = {}
    for row in out_conn.execute(
        "SELECT url, is_skill, reason FROM validation_results WHERE reason NOT LIKE 'Error:%'"
    ).fetchall():
        existing_results[row[0]] = (row[1], row[2])
    out_conn.close()

    local_results = []
    frontmatter_failures = []
    uncached = {}            # cache_key -> (content, [urls])
    skipped_db = 0
    no_frontmatter = 0
    t_start = time.monotonic()
    for url in tqdm(to_validate, desc="Pass 1: frontmatter", unit="file"):
        # Skip if already in DB with valid result
        if url in existing_results:
            skipped_db += 1
            continue
        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        content = local_path.read_text(errors='replace')
        if not has_valid_frontmatter(content):
            no_frontmatter += 1
            frontmatter_failures.append(url)
            continue
        prompt = VALIDATION_PROMPT.format(content=content)
        cache_key = make_cache_key(prompt, model, base_url)
        entry_cache = llm_cache / f"{cache_key}.json"

        async with async_read_cache(entry_cache) as cached:
            if cached is not None:
                local_results.append((url, cached["is_skill"], cached.get("reason", "")))
                continue

        # Deduplicate by content -- same content across repos only needs one API call
        if cache_key in uncached:
            uncached[cache_key][1].append(url)
        else:
            uncached[cache_key] = (content, [url])

    total_uncached = 0
    for k in uncached.keys():
        _, urls = uncached[k]
        total_uncached += len(urls)

    t_pass1 = time.monotonic() - t_start
    print(f"\nPass 1 - frontmatter check ({t_pass1:.1f}s):")
    print(f"  Already validated: {skipped_db:,}")
    print(f"  No valid frontmatter: {no_frontmatter:,}")
    print(f"Pass 2 - LLM classification:")
    print(f"  Cached (no API call): {len(local_results):,}")
    print(f"  Need LLM call: {total_uncached:,}")

    # Write frontmatter failures and cached results to DB
    out_conn = sqlite3.connect(args.output_db)
    for i, url in enumerate(frontmatter_failures):
        out_conn.execute(
            "INSERT OR REPLACE INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 0, 0, 'No valid YAML frontmatter')",
            (url,)
        )
        if (i + 1) % 1000 == 0:
            out_conn.commit()
    for url, is_skill, reason in local_results:
        out_conn.execute(
            "INSERT OR REPLACE INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, ?, ?)",
            (url, is_skill, reason)
        )
    out_conn.commit()
    out_conn.close()

    if not uncached:
        conn = sqlite3.connect(args.output_db)
        final_valid = conn.execute("SELECT COUNT(*) FROM validation_results WHERE is_skill = 1").fetchone()[0]
        conn.close()
        print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
        return

    # --- Phase 2: Concurrent API calls ---
    concurrency = getattr(args, 'concurrency', DEFAULT_CONCURRENCY)
    backend = getattr(args, 'backend', 'anthropic')
    semaphore = asyncio.Semaphore(concurrency)

    if backend == 'claude-agent-sdk':
        try:
            from claude_agent_sdk import ClaudeAgentOptions, AssistantMessage, TextBlock, ResultMessage, query as agent_query
        except ImportError:
            raise ImportError("claude-agent-sdk not installed. Install with: uv add claude-agent-sdk")

        async def validate_one(cache_key, content):
            async with semaphore:
                prompt = VALIDATION_PROMPT.format(content=content)
                text = ""
                opts = ClaudeAgentOptions(max_turns=1)
                async for message in agent_query(prompt=prompt, options=opts):
                    if isinstance(message, ResultMessage) and message.is_error:
                        raise Exception(message.result)
                    if not isinstance(message, AssistantMessage):
                        continue
                    if not hasattr(message, "content"):
                        continue
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            text += block.text
                return parse_response(text.strip())
    else:
        import anthropic
        client_kwargs = {}
        if base_url:
            client_kwargs["base_url"] = base_url
            client_kwargs["api_key"] = "sk-ant-dummy-key-for-local-endpoint"
        client = anthropic.AsyncAnthropic(**client_kwargs)

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
        """Validate and return result with metadata, retrying up to 3 times."""
        last_error = None
        for attempt in range(3):
            try:
                result = await validate_one(cache_key, content)
                is_skill = result.get("is_skill", False)
                reason = result.get("reason", "")
                return cache_key, urls, is_skill, reason, None
            except Exception as e:
                last_error = e
                if attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1))  # Longer backoff: 2s, 4s
        return cache_key, urls, False, f"Error: {str(last_error)[:80]}", last_error

    # Launch all tasks - semaphore controls concurrency
    tasks = [
        asyncio.create_task(process_one(cache_key, content, urls))
        for cache_key, (content, urls) in unique_items
    ]

    t_start = time.monotonic()
    bar = tqdm(asyncio.as_completed(tasks), total=total, desc="Pass 2: LLM", unit="file")
    for coro in bar:
        cache_key, urls, is_skill, reason, error = await coro
        completed += 1

        if error and first_error is None:
            first_error = error
            tqdm.write(f"First error: {first_error}")

        if error:
            error_count += 1
        else:
            # Only cache successful results, not errors
            entry_cache = llm_cache / f"{cache_key}.json"
            await async_write_cache(entry_cache, {"is_skill": is_skill, "reason": reason})

        for url in urls:
            out_conn.execute(
                "INSERT OR REPLACE INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, ?, ?)",
                (url, is_skill, reason)
            )
            if is_skill:
                valid_count += 1
            else:
                invalid_count += 1

        bar.set_postfix(valid=valid_count, rejected=invalid_count, errors=error_count)

        # Commit every 100
        if completed % 100 == 0:
            out_conn.commit()

    out_conn.commit()
    out_conn.close()
    t_pass2 = time.monotonic() - t_start
    print(f"Done in {t_pass2:.1f}s: valid={valid_count:,}, rejected={invalid_count:,}, errors={error_count:,}")

    conn = sqlite3.connect(args.output_db)
    final_valid = conn.execute("SELECT COUNT(*) FROM validation_results WHERE is_skill = 1").fetchone()[0]
    conn.close()
    print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
