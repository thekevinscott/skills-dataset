"""Filter valid SKILL.md files using Claude via the Message Batches API (50% discount)."""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path

import anthropic

VALIDATION_PROMPT = """Analyze this SKILL.md file from GitHub.

A valid Claude Code skill file has:
1. YAML frontmatter between --- markers (at the start)
2. Markdown content after frontmatter
3. Content that extends Claude's capabilities (instructions, workflows, knowledge, or commands)

Common frontmatter fields (all optional):
- name, description, disable-model-invocation, user-invocable, allowed-tools

The content can be:
- Reference material (API conventions, patterns, style guides)
- Task instructions (step-by-step workflows like deploy, commit)
- Templates or examples
- Configuration for tools/agents

Be INCLUSIVE - if it has frontmatter + markdown content that looks skill-like, mark as valid.
Reject only if clearly not a skill (blog posts, GitHub templates, unrelated docs).

File content:
{content}

Respond with JSON only:
{{"is_skill": true/false, "reason": "one sentence"}}"""

DEFAULT_MODEL = "claude-haiku-4-5-20251001"
CONTENT_MAX_BYTES = 3000      # Truncate for classification; frontmatter + intro is enough
CACHE_DIR = Path.home() / ".cache/skills-dataset/claude"
BATCH_CHUNK_SIZE = 10_000     # Max requests per Batches API call


def has_valid_frontmatter(content: str) -> bool:
    """Check if content has valid YAML frontmatter."""
    if not content.startswith('---'):
        return False
    parts = content.split('---', 2)
    if len(parts) < 3:
        return False
    try:
        import yaml
        yaml.safe_load(parts[1])
        return True
    except Exception:
        return False


def truncate_content(content: str, max_bytes: int = CONTENT_MAX_BYTES) -> str:
    """Truncate content to max_bytes, preserving valid UTF-8."""
    encoded = content.encode('utf-8')
    if len(encoded) <= max_bytes:
        return content
    return encoded[:max_bytes].decode('utf-8', errors='ignore') + "\n[truncated]"


def prompt_hash(content: str) -> str:
    """Hash the full formatted prompt for cache keying."""
    full_prompt = VALIDATION_PROMPT.format(content=content)
    return hashlib.sha256(full_prompt.encode()).hexdigest()


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


def parse_github_url(url: str) -> tuple[str, str, str, str] | None:
    """Parse GitHub URL into (owner, repo, ref, path)."""
    parts = url.split('/')
    if len(parts) < 8 or parts[2] != 'github.com' or parts[5] != 'blob':
        return None
    return parts[3], parts[4], parts[6], '/'.join(parts[7:])


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


async def main(args):
    """Main filter pipeline using Anthropic Message Batches API (50% discount)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    init_output_db(args.output_db)
    model = args.model or DEFAULT_MODEL

    # Get all URLs from source DB
    main_conn = sqlite3.connect(args.main_db)
    all_urls = [row[0] for row in main_conn.execute("SELECT url FROM files").fetchall()]
    main_conn.close()

    # Already validated
    out_conn = sqlite3.connect(args.output_db)
    validated_urls = {row[0] for row in out_conn.execute(
        "SELECT url FROM validation_results"
    ).fetchall()}
    out_conn.close()

    # Content on disk
    content_paths = set()
    for dirpath, _, filenames in os.walk(args.content_dir):
        for fname in filenames:
            content_paths.add(os.path.join(dirpath, fname))

    to_validate = []
    no_content = 0
    for url in all_urls:
        if url in validated_urls:
            continue
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        if str(resolve_content_path(args.content_dir, owner, repo, ref, path)) in content_paths:
            to_validate.append(url)
        else:
            no_content += 1

    print(f"Total: {len(all_urls):,}, Already validated: {len(validated_urls):,}, "
          f"Content available: {len(to_validate):,}, No content yet: {no_content:,}")

    if not to_validate:
        valid_count = rebuild_files_table(args.main_db, args.output_db)
        print(f"\nOutput DB: {args.output_db} ({valid_count:,} valid skill files)")
        return

    # --- Phase 1: Read, frontmatter filter, truncate, check cache ---
    local_results = []       # (url, is_skill, reason) -- immediate results
    uncached = {}            # cache_key -> (truncated_content, [urls])
    stats = {"frontmatter_rejected": 0, "cached": 0, "deduplicated": 0, "read_error": 0}

    for url in to_validate:
        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)

        try:
            content = local_path.read_text(errors='replace')
        except Exception:
            stats["read_error"] += 1
            local_results.append((url, False, "File read error"))
            continue

        if not has_valid_frontmatter(content):
            stats["frontmatter_rejected"] += 1
            local_results.append((url, False, "No valid YAML frontmatter"))
            continue

        truncated = truncate_content(content)
        cache_key = prompt_hash(truncated)
        cached = get_cached_result(cache_key)

        if cached is not None:
            stats["cached"] += 1
            local_results.append((url, cached["is_skill"], cached.get("reason", "")))
            continue

        # Deduplicate by content -- same content across repos only needs one API call
        if cache_key in uncached:
            uncached[cache_key][1].append(url)
            stats["deduplicated"] += 1
        else:
            uncached[cache_key] = (truncated, [url])

    print(f"Frontmatter rejected: {stats['frontmatter_rejected']:,}, "
          f"Cached: {stats['cached']:,}, "
          f"Unique to submit: {len(uncached):,}, "
          f"Deduplicated: {stats['deduplicated']:,}")

    # Write local results to DB immediately
    out_conn = sqlite3.connect(args.output_db)
    for url, is_skill, reason in local_results:
        out_conn.execute(
            "INSERT OR REPLACE INTO validation_results (url, is_skill, reason) VALUES (?, ?, ?)",
            (url, is_skill, reason)
        )
    out_conn.commit()
    out_conn.close()

    if not uncached:
        valid_count = rebuild_files_table(args.main_db, args.output_db)
        print(f"\nOutput DB: {args.output_db} ({valid_count:,} valid skill files)")
        return

    # --- Phase 2: Submit to Batches API ---
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

    # --- Phase 4: Rebuild files table ---
    final_valid = rebuild_files_table(args.main_db, args.output_db)
    print(f"\nOutput DB: {args.output_db} ({final_valid:,} valid skill files)")
