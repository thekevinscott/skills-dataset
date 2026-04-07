"""Pass 2: Heuristic rejection rules for SKILL.md files.

Only rejects are reliable -- these rules have high precision for identifying
non-skills but should never be used to confirm something IS a skill.
"""

import re
import yaml


def parse_frontmatter(content: str) -> dict:
    """Extract frontmatter dict from content."""
    match = re.match(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}
    try:
        result = yaml.safe_load(match.group(1))
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def get_body(content: str) -> str:
    """Get content after frontmatter."""
    match = re.match(r'^---\s*\n.*?\n---\s*\n?(.*)', content, re.DOTALL)
    return match.group(1) if match else content


def heuristic_reject(content: str) -> tuple[bool, str]:
    """Check if content is definitely NOT a skill.

    Returns (is_rejected, reason). Only trust rejections (is_rejected=True).
    A False result means "uncertain", not "is a skill".
    """
    fm = parse_frontmatter(content)
    body = get_body(content).strip()
    fm_str = str(fm).lower()

    # Prompt injection patterns
    if '<agent-activation' in content.lower():
        return True, "prompt injection: <agent-activation> pattern"

    # Academic papers (skillXiv)
    if 'skillxiv' in fm_str:
        return True, "academic paper: skillxiv engine"
    if 'arxiv' in str(fm.get('url', '')).lower():
        return True, "academic paper: arxiv URL in frontmatter"

    # Issue templates
    if all(k in fm for k in ['about', 'labels', 'assignees']):
        return True, "GitHub issue template"

    # Commercial/sales content
    if any(k in fm for k in ['price', 'revenue_potential']):
        return True, "commercial/sales content"

    # Non-Claude platform
    platform = str(fm.get('platform', '')).lower()
    if platform and 'claude' not in platform:
        return True, f"non-Claude platform: {fm.get('platform')}"

    # Empty body
    if len(body) < 50:
        return True, "empty or near-empty body"

    # Tool documentation cards (emoji + github_url + triggers, no sections)
    if all(k in fm for k in ['emoji', 'github_url', 'triggers']) and 'name' not in fm:
        return True, "tool documentation card"

    # Blog posts (title + date, no name field)
    if 'title' in fm and 'name' not in fm and re.search(r'date', fm_str):
        if any(k in fm for k in ['categories', 'tags', 'layout', 'hero', 'author', 'authors']):
            return True, "blog post"

    # Not rejected -- uncertain
    return False, ""


async def heuristic_pass(args):
    """Run heuristic rejection on all files with frontmatter.

    Re-runs every time (rules may change). Sets heuristic_reject (0 or 1)
    and heuristic_reason on validation_results.
    """
    from .filter import init_output_db, open_db, scan_content, resolve_content_path
    from .parse_github_url import parse_github_url
    from tqdm import tqdm

    init_output_db(args.output_db)
    _, to_validate, _ = scan_content(args)

    conn = open_db(args.output_db)
    has_fm = set(
        row[0] for row in conn.execute(
            "SELECT url FROM validation_results WHERE has_frontmatter = 1"
        ).fetchall()
    )
    conn.close()

    urls = [url for url in to_validate if url in has_fm]
    print(f"  Running heuristics on {len(urls):,} URLs...")

    conn = open_db(args.output_db)
    rejected = 0
    checked = 0
    for url in tqdm(urls, desc="Pass 2: heuristics", unit="url"):
        parsed = parse_github_url(url)
        if not parsed:
            continue
        owner, repo, ref, path = parsed
        local_path = resolve_content_path(args.content_dir, owner, repo, ref, path)
        if not local_path.exists():
            continue
        content = local_path.read_text(errors='replace')
        is_rejected, reason = heuristic_reject(content)
        conn.execute(
            "UPDATE validation_results SET heuristic_reject = ?, heuristic_reason = ? WHERE url = ?",
            (1 if is_rejected else 0, reason if reason else None, url)
        )
        if is_rejected:
            rejected += 1
        checked += 1
        if checked % 5000 == 0:
            conn.commit()

    conn.commit()
    conn.close()
    print(f"  Checked: {checked:,}, Rejected: {rejected:,}")
