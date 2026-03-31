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
        return yaml.safe_load(match.group(1)) or {}
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
