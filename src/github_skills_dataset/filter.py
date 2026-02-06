"""Filter valid SKILL.md files using Claude."""

import asyncio
import hashlib
import json
import re
import sqlite3
from pathlib import Path
import anthropic

# System prompt (cached across all API calls)
SYSTEM_PROMPT = """Analyze SKILL.md files from GitHub and classify them as valid or invalid.

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

Respond with JSON only:
{"is_skill": true/false, "reason": "one sentence"}"""

# Per-file user prompt (only the file content varies)
USER_PROMPT = """File content:
{content}"""

# Legacy combined prompt kept for cache key compatibility only
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
    except:
        return False

CACHE_DIR = Path.home() / ".cache/skills-dataset/claude"

DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# Batching: pack multiple files into one API call to reduce overhead
BATCH_TOKEN_BUDGET = 30_000  # Max estimated content tokens per API call
BATCH_MAX_FILES = 20         # Cap for reliable JSON array parsing
BYTES_PER_TOKEN = 4          # Rough bytes-to-tokens ratio


def prompt_hash(content: str) -> str:
    """Hash the full formatted prompt for cache keying."""
    full_prompt = VALIDATION_PROMPT.format(content=content)
    return hashlib.sha256(full_prompt.encode()).hexdigest()


def get_cached_result(content_hash: str) -> dict | None:
    """Check file cache for a previous Claude result."""
    cache_file = CACHE_DIR / f"{content_hash}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    return None


def insert_cached_result(content_hash: str, is_skill: bool, reason: str):
    """Store Claude result in file cache."""
    cache_file = CACHE_DIR / f"{content_hash}.json"
    cache_file.write_text(json.dumps({"is_skill": is_skill, "reason": reason}))


BATCH_SYSTEM_PROMPT = """Analyze each numbered SKILL.md file and classify as valid or invalid.

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

Respond with a JSON array only, one object per file in input order:
[{"file": 1, "is_skill": true, "reason": "one sentence"}, ...]"""


def make_batch_user_prompt(file_contents: list[tuple[str, str]]) -> str:
    """Format multiple files into a single user prompt."""
    parts = []
    for i, (_url, content) in enumerate(file_contents, 1):
        parts.append(f"=== File {i} ===\n{content}")
    return f"Classify these {len(file_contents)} files:\n\n" + "\n\n".join(parts)


def pack_batches(items: list[tuple[str, str, int]]) -> list[list[tuple[str, str]]]:
    """Pack (url, content, size_bytes) into batches respecting token budget and file count."""
    batches = []
    current_batch = []
    current_tokens = 0

    for url, content, size_bytes in items:
        estimated_tokens = size_bytes // BYTES_PER_TOKEN

        # Single file exceeds budget -- send it alone
        if estimated_tokens > BATCH_TOKEN_BUDGET:
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0
            batches.append([(url, content)])
            continue

        if current_tokens + estimated_tokens > BATCH_TOKEN_BUDGET or len(current_batch) >= BATCH_MAX_FILES:
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0

        current_batch.append((url, content))
        current_tokens += estimated_tokens

    if current_batch:
        batches.append(current_batch)

    return batches


def parse_github_url(url: str) -> tuple[str, str, str, str] | None:
    """Parse GitHub URL into (owner, repo, ref, path)."""
    parts = url.split('/')
    if len(parts) < 8 or parts[2] != 'github.com' or parts[5] != 'blob':
        return None
    owner, repo, ref = parts[3], parts[4], parts[6]
    path = '/'.join(parts[7:])
    return owner, repo, ref, path


def resolve_content_path(content_dir: Path, owner: str, repo: str, ref: str, path: str) -> Path:
    """Build path to local content file."""
    return content_dir / owner / repo / "blob" / ref / path


_client = None

def _get_client():
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic()
    return _client


async def validate_file(url: str, content: str, model: str = DEFAULT_MODEL) -> dict:
    """Validate single file using Claude. Results are cached transparently."""
    cache_key = prompt_hash(content)
    cached = get_cached_result(cache_key)
    if cached is not None:
        return cached

    prompt = VALIDATION_PROMPT.format(content=content)

    try:
        message = await _get_client().messages.create(
            model=model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )

        result_text = message.content[0].text if message.content else ""

        if not result_text.strip():
            raise ValueError("Claude returned empty response")

        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            match = re.search(r'```json\s*(\{.*?\})\s*```', result_text, re.DOTALL)
            if match:
                result = json.loads(match.group(1))
            else:
                match = re.search(r'\{.*"is_skill".*\}', result_text, re.DOTALL)
                if match:
                    result = json.loads(match.group(0))
                else:
                    raise ValueError(f"Could not parse JSON from response: {result_text[:200]}")

        insert_cached_result(cache_key, result.get("is_skill", False), result.get("reason", ""))
        return result

    except Exception as e:
        raise RuntimeError(f"Claude API error for {url}: {str(e)}") from e


async def validate_batch_files(file_contents: list[tuple[str, str]], model: str = DEFAULT_MODEL) -> list[dict]:
    """Validate multiple files in a single API call. Checks/populates per-file cache."""
    results = [None] * len(file_contents)
    uncached = []  # (original_index, url, content)

    for i, (url, content) in enumerate(file_contents):
        cache_key = prompt_hash(content)
        cached = get_cached_result(cache_key)
        if cached is not None:
            cached["url"] = url
            results[i] = cached
        else:
            uncached.append((i, url, content))

    if not uncached:
        return results

    # Single uncached file -- use existing single-file path
    if len(uncached) == 1:
        idx, url, content = uncached[0]
        result = await validate_file(url, content, model=model)
        result["url"] = url
        results[idx] = result
        return results

    # Build batch prompt for uncached files
    batch_contents = [(url, content) for _, url, content in uncached]
    prompt = make_batch_user_prompt(batch_contents)

    try:
        message = await _get_client().messages.create(
            model=model,
            max_tokens=max(256, 64 * len(uncached)),
            system=BATCH_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        result_text = message.content[0].text if message.content else ""
        if not result_text.strip():
            raise ValueError("Claude returned empty response")

        try:
            parsed = json.loads(result_text)
        except json.JSONDecodeError:
            match = re.search(r'```json\s*(\[.*?\])\s*```', result_text, re.DOTALL)
            if match:
                parsed = json.loads(match.group(1))
            else:
                match = re.search(r'\[.*\]', result_text, re.DOTALL)
                if match:
                    parsed = json.loads(match.group(0))
                else:
                    raise ValueError(f"Could not parse JSON array: {result_text[:200]}")

        if not isinstance(parsed, list) or len(parsed) != len(uncached):
            raise ValueError(
                f"Expected {len(uncached)} results, got "
                f"{len(parsed) if isinstance(parsed, list) else 'non-list'}"
            )

        for j, (idx, url, content) in enumerate(uncached):
            file_result = parsed[j]
            is_skill = file_result.get("is_skill", False)
            reason = file_result.get("reason", "")
            cache_key = prompt_hash(content)
            insert_cached_result(cache_key, is_skill, reason)
            results[idx] = {"url": url, "is_skill": is_skill, "reason": reason}

        return results

    except Exception as e:
        # Batch failed -- fall back to individual validation
        print(f"  Batch API call failed ({e}), falling back to individual calls")
        for idx, url, content in uncached:
            try:
                result = await validate_file(url, content, model=model)
                result["url"] = url
                results[idx] = result
            except Exception as inner_e:
                results[idx] = {"url": url, "is_skill": False, "reason": f"Error: {str(inner_e)}"}
        return results


async def process_batch(urls: list[str], content_dir: Path, semaphore: asyncio.Semaphore, model: str = DEFAULT_MODEL) -> list[dict]:
    """Process URLs: filter by frontmatter locally, then batch API calls."""
    items = []   # (url, content, size_bytes) for files needing API validation
    results = [] # immediate results for locally-rejected files

    for url in urls:
        parsed = parse_github_url(url)
        if not parsed:
            results.append({"url": url, "is_skill": False, "reason": "Invalid URL format"})
            continue

        owner, repo, ref, path = parsed
        local_path = resolve_content_path(content_dir, owner, repo, ref, path)
        try:
            content = local_path.read_text(errors='replace')
        except Exception:
            results.append({"url": url, "is_skill": False, "reason": "File read error"})
            continue

        if not has_valid_frontmatter(content):
            results.append({"url": url, "is_skill": False, "reason": "No valid YAML frontmatter"})
            continue

        items.append((url, content, len(content.encode('utf-8'))))

    if not items:
        return results

    # Pack into token-budget-sized API batches
    api_batches = pack_batches(items)

    async def call_batch(batch):
        async with semaphore:
            return await validate_batch_files(batch, model=model)

    tasks = [call_batch(b) for b in api_batches]
    batch_results = await asyncio.gather(*tasks)

    for batch_result in batch_results:
        results.extend(batch_result)

    return results


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
    """Main filter pipeline: validate files, produce filtered DB."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    init_output_db(args.output_db)

    # Get all URLs from source DB
    main_conn = sqlite3.connect(args.main_db)
    all_urls = [row[0] for row in main_conn.execute("SELECT url FROM files").fetchall()]
    main_conn.close()

    # Check what's already validated in output DB
    out_conn = sqlite3.connect(args.output_db)
    validated_urls = {row[0] for row in out_conn.execute("SELECT url FROM validation_results").fetchall()}
    out_conn.close()

    # Build set of all content paths on disk (single os.walk, fast)
    import os
    content_paths = set()
    for dirpath, _, filenames in os.walk(args.content_dir):
        for fname in filenames:
            content_paths.add(os.path.join(dirpath, fname))

    # Only include URLs that have content on disk and haven't been validated yet
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

    print(f"Total: {len(all_urls):,}, Already validated: {len(validated_urls):,}, Content available: {len(to_validate):,}, No content yet: {no_content:,}")

    if to_validate:
        semaphore = asyncio.Semaphore(args.max_concurrent)
        stats = {"validated": 0, "is_skill": 0, "not_skill": 0}

        for i in range(0, len(to_validate), args.batch_size):
            batch = to_validate[i:i + args.batch_size]
            print(f"\nBatch {i // args.batch_size + 1} ({len(batch)} files):")

            results = await process_batch(batch, args.content_dir, semaphore, model=args.model or DEFAULT_MODEL)

            out_conn = sqlite3.connect(args.output_db)
            for result in results:
                url = result["url"]
                is_skill = result.get("is_skill", False)
                reason = result.get("reason", "")

                out_conn.execute(
                    "INSERT OR REPLACE INTO validation_results (url, is_skill, reason) VALUES (?, ?, ?)",
                    (url, is_skill, reason)
                )

                stats["validated"] += 1
                if is_skill:
                    stats["is_skill"] += 1
                    print(f"  + {url[:80]}")
                else:
                    stats["not_skill"] += 1
                    print(f"  - {url[:80]} -- {reason[:40]}")

            out_conn.commit()
            out_conn.close()

        print(f"\nValidated: {stats['validated']}, valid: {stats['is_skill']}, rejected: {stats['not_skill']}")

    # Rebuild files table with valid URLs
    valid_count = rebuild_files_table(args.main_db, args.output_db)
    print(f"\nOutput DB: {args.output_db} ({valid_count:,} valid skill files)")
