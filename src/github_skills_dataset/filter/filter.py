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
from .parse_github_url import parse_github_url
from .has_valid_frontmatter import has_valid_frontmatter

DEFAULT_CONCURRENCY = 10


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
    else:
        conn.execute("""
            CREATE TABLE llm_skill_evaluation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
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


async def filter_pass2(args, to_validate=None):
    """Pass 5 (LLM): classify files via LLM. Optionally filtered by confidence threshold."""
    init_output_db(args.output_db)
    model = args.model or DEFAULT_MODEL
    base_url = getattr(args, 'base_url', None)
    backend = getattr(args, 'backend', 'anthropic')
    confidence_threshold = getattr(args, 'confidence_threshold', None)

    if to_validate is None:
        _, to_validate, _ = scan_content(args)

    # Skip: frontmatter failures, heuristic rejects, confident embeddings
    out_conn = open_db(args.output_db)
    skip_urls = set()

    # Frontmatter failures
    for row in out_conn.execute("SELECT url FROM validation_results WHERE has_frontmatter = 0"):
        skip_urls.add(row[0])

    # Heuristic rejects
    for row in out_conn.execute("SELECT url FROM validation_results WHERE heuristic_reject = 1"):
        skip_urls.add(row[0])

    # Confident embedding predictions (above threshold)
    if confidence_threshold is not None:
        for row in out_conn.execute(
            "SELECT url FROM validation_results WHERE classifier_confidence >= ?",
            (confidence_threshold,)
        ):
            skip_urls.add(row[0])

    out_conn.close()

    to_classify = [url for url in to_validate if url not in skip_urls]
    skipped = len(to_validate) - len(to_classify)

    limit = getattr(args, 'limit', None)
    if limit is not None:
        to_classify = to_classify[:limit]

    print(f"  Already done: {skipped:,}, to classify: {len(to_classify):,}")

    local_results = []
    uncached = {}
    t_start = time.monotonic()
    for url in tqdm(to_classify, desc="Pass 2: prep", unit="file"):
        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        content = local_path.read_text(errors='replace')
        if not has_valid_frontmatter(content):
            continue
        prompt = VALIDATION_PROMPT.format(content=content)
        cache_key = make_cache_key(prompt, model, base_url)
        entry_cache = llm_cache / f"{cache_key}.json"

        async with async_read_cache(entry_cache) as cached:
            if cached is not None:
                local_results.append((url, cached["is_skill"], cached.get("reason", "")))
                continue

        if cache_key in uncached:
            uncached[cache_key][1].append(url)
        else:
            uncached[cache_key] = (content, [url])

    total_uncached = sum(len(urls) for _, urls in uncached.values())

    t_prep = time.monotonic() - t_start
    print(f"\nPass 2 - LLM classification ({t_prep:.1f}s prep):")
    print(f"  Cached (no API call): {len(local_results):,}")
    print(f"  Need LLM call: {total_uncached:,}")

    out_conn = open_db(args.output_db)
    for url, is_skill, reason in local_results:
        out_conn.execute(
            "INSERT OR IGNORE INTO llm_skill_evaluation (url, backend, model, base_url, is_skill, reason) VALUES (?, ?, ?, ?, ?, ?)",
            (url, backend, model, base_url, is_skill, reason)
        )
    out_conn.commit()
    out_conn.close()

    if not uncached:
        conn = open_db(args.output_db)
        final_valid = conn.execute(
            "SELECT COUNT(*) FROM llm_skill_evaluation WHERE is_skill = 1 AND backend = ? AND model = ?",
            (backend, model)
        ).fetchone()[0]
        conn.close()
        print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files for {backend}/{model})")
        return

    # --- Concurrent API calls ---
    concurrency = getattr(args, 'concurrency', DEFAULT_CONCURRENCY)
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
                opts = ClaudeAgentOptions(max_turns=1, model=model)
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
    elif backend == 'openai':
        from openai import AsyncOpenAI
        client_kwargs = {}
        if base_url:
            client_kwargs["base_url"] = base_url
        client_kwargs["api_key"] = os.environ.get("OPENAI_API_KEY", "dummy")
        client = AsyncOpenAI(**client_kwargs)

        async def validate_one(cache_key, content):
            async with semaphore:
                prompt = VALIDATION_PROMPT.format(content=content)
                response = await client.chat.completions.create(
                    model=model,
                    max_tokens=256,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.choices[0].message.content
                return parse_response(text)
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
    out_conn = open_db(args.output_db)
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
                    await asyncio.sleep(2 * (attempt + 1))
        return cache_key, urls, False, f"Error: {str(last_error)[:80]}", last_error

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
            entry_cache = llm_cache / f"{cache_key}.json"
            await async_write_cache(entry_cache, {"is_skill": is_skill, "reason": reason})

        for url in urls:
            out_conn.execute(
                "INSERT OR REPLACE INTO llm_skill_evaluation (url, backend, model, base_url, is_skill, reason) VALUES (?, ?, ?, ?, ?, ?)",
                (url, backend, model, base_url, is_skill, reason)
            )
            if is_skill:
                valid_count += 1
            else:
                invalid_count += 1

        bar.set_postfix(valid=valid_count, rejected=invalid_count, errors=error_count)

        if completed % 100 == 0:
            out_conn.commit()

    out_conn.commit()
    out_conn.close()
    t_pass2 = time.monotonic() - t_start
    print(f"Done in {t_pass2:.1f}s: valid={valid_count:,}, rejected={invalid_count:,}, errors={error_count:,}")

    conn = open_db(args.output_db)
    final_valid = conn.execute(
        "SELECT COUNT(*) FROM llm_skill_evaluation WHERE is_skill = 1 AND backend = ? AND model = ?",
        (backend, model)
    ).fetchone()[0]
    conn.close()
    print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files for {backend}/{model})")


async def filter(args):
    """Run both passes in sequence, scanning content once."""
    init_output_db(args.output_db)
    _, to_validate, _ = scan_content(args)
    await filter_pass1(args, to_validate=to_validate)
    await filter_pass2(args, to_validate=to_validate)
