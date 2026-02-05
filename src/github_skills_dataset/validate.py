"""Validate SKILL.md files using Claude."""

import asyncio
import json
import sqlite3
from pathlib import Path
from claude_agent_sdk import query, ClaudeAgentOptions

# Validation prompt based on Claude Code skill documentation
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

    # Find closing ---
    parts = content.split('---', 2)
    if len(parts) < 3:
        return False

    # Try to parse YAML
    try:
        import yaml
        yaml.safe_load(parts[1])
        return True
    except:
        return False

def init_validation_db(db_path: Path):
    """Create validation database."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_results (
            url TEXT PRIMARY KEY,
            is_skill BOOLEAN NOT NULL,
            reason TEXT,
            validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def get_all_file_urls(main_db: Path) -> list[str]:
    """Get all URLs from github-data-file-fetcher database."""
    conn = sqlite3.connect(main_db)
    cursor = conn.execute("SELECT url FROM files")
    urls = [row[0] for row in cursor.fetchall()]
    conn.close()
    return urls

def get_validated_urls(validation_db: Path) -> set[str]:
    """Get already validated URLs."""
    if not validation_db.exists():
        return set()
    conn = sqlite3.connect(validation_db)
    cursor = conn.execute("SELECT url FROM validation_results")
    urls = {row[0] for row in cursor.fetchall()}
    conn.close()
    return urls

def insert_validation_result(validation_db: Path, url: str, is_skill: bool, reason: str):
    """Insert validation result."""
    conn = sqlite3.connect(validation_db)
    conn.execute(
        "INSERT OR REPLACE INTO validation_results (url, is_skill, reason) VALUES (?, ?, ?)",
        (url, is_skill, reason)
    )
    conn.commit()
    conn.close()

def parse_github_url(url: str) -> tuple[str, str, str, str] | None:
    """Parse GitHub URL into (owner, repo, ref, path)."""
    # https://github.com/owner/repo/blob/ref/path
    parts = url.split('/')
    if len(parts) < 8 or parts[2] != 'github.com' or parts[5] != 'blob':
        return None
    owner, repo, ref = parts[3], parts[4], parts[6]
    path = '/'.join(parts[7:])
    return owner, repo, ref, path

def resolve_content_path(content_dir: Path, owner: str, repo: str, ref: str, path: str) -> Path:
    """Build path to local content file."""
    return content_dir / owner / repo / "blob" / ref / path

async def validate_file(url: str, content: str) -> dict:
    """Validate single file using Claude. Raises on API errors."""
    prompt = VALIDATION_PROMPT.format(content=content)  # Full content, no truncation

    options = ClaudeAgentOptions(permission_mode='bypassPermissions')

    result_text = ""
    try:
        async for message in query(prompt=prompt, options=options):
            if hasattr(message, 'content'):
                for block in message.content:
                    if hasattr(block, 'text'):
                        result_text += block.text

        # Parse JSON response
        import re
        # Try direct JSON parse first
        try:
            return json.loads(result_text)
        except json.JSONDecodeError:
            # Extract from markdown code block
            match = re.search(r'```json\s*(\{.*?\})\s*```', result_text, re.DOTALL)
            if match:
                return json.loads(match.group(1))
            # Try to find any JSON object
            match = re.search(r'\{.*"is_skill".*\}', result_text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
            raise ValueError(f"Could not parse JSON from response: {result_text[:200]}")

    except Exception as e:
        # Raise on API errors - don't swallow them
        raise RuntimeError(f"Claude API error for {url}: {str(e)}") from e

async def process_batch(urls: list[str], content_dir: Path, semaphore: asyncio.Semaphore) -> list[dict]:
    """Process batch of URLs with concurrency limit."""
    async def process_one(url: str):
        async with semaphore:
            try:
                parsed = parse_github_url(url)
                if not parsed:
                    return {"url": url, "is_skill": False, "reason": "Invalid URL format"}

                owner, repo, ref, path = parsed
                local_path = resolve_content_path(content_dir, owner, repo, ref, path)

                if not local_path.exists():
                    return {"url": url, "is_skill": False, "reason": "File not found"}

                content = local_path.read_text(errors='replace')

                # First pass: Check for valid frontmatter (cheap)
                if not has_valid_frontmatter(content):
                    return {"url": url, "is_skill": False, "reason": "No valid YAML frontmatter"}

                # Second pass: Ask Claude (expensive)
                result = await validate_file(url, content)
                result["url"] = url
                return result

            except RuntimeError:
                # Re-raise API errors (don't catch them)
                raise
            except Exception as e:
                return {"url": url, "is_skill": False, "reason": f"Error: {str(e)}"}

    tasks = [process_one(url) for url in urls]
    return await asyncio.gather(*tasks)

async def main(args):
    """Main validation pipeline."""

    # Initialize validation database
    init_validation_db(args.validation_db)

    # Get files to validate
    all_urls = get_all_file_urls(args.main_db)
    validated_urls = get_validated_urls(args.validation_db)
    to_validate = [url for url in all_urls if url not in validated_urls]

    print(f"Total: {len(all_urls):,}, Already validated: {len(validated_urls):,}, To validate: {len(to_validate):,}")

    if not to_validate:
        print("Nothing to validate!")
        return

    # Process in batches
    semaphore = asyncio.Semaphore(args.max_concurrent)
    stats = {"validated": 0, "is_skill": 0, "not_skill": 0}

    for i in range(0, len(to_validate), args.batch_size):
        batch = to_validate[i:i + args.batch_size]
        print(f"\nBatch {i // args.batch_size + 1} ({len(batch)} files):")

        results = await process_batch(batch, args.content_dir, semaphore)

        # Save results
        for result in results:
            url = result["url"]
            is_skill = result.get("is_skill", False)
            reason = result.get("reason", "")

            insert_validation_result(args.validation_db, url, is_skill, reason)

            stats["validated"] += 1
            if is_skill:
                stats["is_skill"] += 1
                print(f"  ✓ {url[:80]}")
            else:
                stats["not_skill"] += 1
                print(f"  ✗ {url[:80]} - {reason[:40]}")

    print(f"\n✅ Done: {stats['validated']} validated, {stats['is_skill']} valid skills, {stats['not_skill']} false positives")
