"""Generate training data by sending unlabeled content to an LLM.

Reads training/labeled.csv for skip logic (already-labeled content hashes).
Scans content directory for all files. Sends unlabeled content to the LLM.
Appends results to the CSV.
"""

import asyncio
import csv
import os
from pathlib import Path

from tqdm import tqdm

from ..hash import content_hash_file
from .config import CONTENT_MAX_BYTES, DEFAULT_MODEL, VALIDATION_PROMPT
from .filter import init_output_db, open_db, scan_content, resolve_content_path
from .parse_github_url import parse_github_url


def _load_labeled_hashes(csv_path: Path) -> set[str]:
    """Load already-labeled content hashes from CSV."""
    if not csv_path.exists():
        return set()
    hashes = set()
    with open(csv_path) as f:
        for line in f:
            if line.startswith("#") or line.startswith("content_hash"):
                continue
            parts = line.strip().split(",")
            if parts:
                hashes.add(parts[0])
    return hashes


def _parse_response(text: str) -> dict:
    """Parse JSON from LLM response."""
    import json
    import re
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


async def generate_training(args):
    """Send unlabeled content to LLM and append results to CSV."""
    init_output_db(args.output_db)
    model = args.model or DEFAULT_MODEL
    base_url = getattr(args, 'base_url', None)
    backend = getattr(args, 'backend', 'anthropic')
    limit = getattr(args, 'limit', None)
    csv_path = Path(getattr(args, 'labeled_csv', 'training/labeled.csv'))

    # Load already-labeled hashes
    labeled_hashes = _load_labeled_hashes(csv_path)
    print(f"Already labeled: {len(labeled_hashes):,} content hashes")

    # Scan content directory
    _, to_validate, _ = scan_content(args)

    # Get URLs with frontmatter
    conn = open_db(args.output_db)
    has_fm = set(
        row[0] for row in conn.execute(
            "SELECT url FROM validation_results WHERE has_frontmatter = 1"
        ).fetchall()
    )
    conn.close()

    # Build content_hash -> content for unlabeled files, deduplicating
    to_label = {}  # content_hash -> content (truncated)
    for url in to_validate:
        if url not in has_fm:
            continue
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        ch = content_hash_file(local_path)
        if ch in labeled_hashes or ch in to_label:
            continue
        content = local_path.read_text(errors='replace')
        to_label[ch] = content[:CONTENT_MAX_BYTES]

    print(f"Unlabeled unique content: {len(to_label):,}")

    if limit is not None:
        items = list(to_label.items())[:limit]
    else:
        items = list(to_label.items())

    print(f"To classify: {len(items):,}")

    if not items:
        print("Nothing to label.")
        return

    # Set up LLM client
    concurrency = getattr(args, 'concurrency', 8)
    semaphore = asyncio.Semaphore(concurrency)

    if backend == 'claude-agent-sdk':
        try:
            from claude_agent_sdk import ClaudeAgentOptions, AssistantMessage, TextBlock, ResultMessage, query as agent_query
        except ImportError:
            raise ImportError("claude-agent-sdk not installed. Install with: uv add claude-agent-sdk")

        async def classify_one(content):
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
                return _parse_response(text.strip())
    elif backend == 'openai':
        from openai import AsyncOpenAI
        client_kwargs = {}
        if base_url:
            client_kwargs["base_url"] = base_url
        client_kwargs["api_key"] = os.environ.get("OPENAI_API_KEY", "dummy")
        client = AsyncOpenAI(**client_kwargs)

        async def classify_one(content):
            async with semaphore:
                prompt = VALIDATION_PROMPT.format(content=content)
                response = await client.chat.completions.create(
                    model=model, max_tokens=64,
                    messages=[{"role": "user", "content": prompt}],
                )
                return _parse_response(response.choices[0].message.content)
    else:
        import anthropic
        client_kwargs = {}
        if base_url:
            client_kwargs["base_url"] = base_url
            client_kwargs["api_key"] = "sk-ant-dummy-key-for-local-endpoint"
        client = anthropic.AsyncAnthropic(**client_kwargs)

        async def classify_one(content):
            async with semaphore:
                prompt = VALIDATION_PROMPT.format(content=content)
                message = await client.messages.create(
                    model=model, max_tokens=64,
                    messages=[{"role": "user", "content": prompt}],
                )
                return _parse_response(message.content[0].text)

    # Process with retries
    async def process_one(ch, content):
        last_error = None
        for attempt in range(3):
            try:
                result = await classify_one(content)
                return ch, result.get("is_skill", False), None
            except Exception as e:
                last_error = e
                if attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1))
        return ch, None, last_error

    tasks = [
        asyncio.create_task(process_one(ch, content))
        for ch, content in items
    ]

    # Collect results
    results = []
    errors = 0
    bar = tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="LLM classify", unit="file")
    for coro in bar:
        ch, is_skill, error = await coro
        if error:
            errors += 1
            tqdm.write(f"Error: {error}")
        else:
            results.append((ch, is_skill))
        bar.set_postfix(done=len(results), errors=errors)

    print(f"\nClassified: {len(results):,}, Errors: {errors:,}")

    # Append to CSV
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()

    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            f.write(f"# backend={backend}, model={model}\n")
            writer.writerow(["content_hash", "is_skill"])
        for ch, is_skill in results:
            writer.writerow([ch, "true" if is_skill else "false"])

    print(f"Appended {len(results):,} rows to {csv_path}")
