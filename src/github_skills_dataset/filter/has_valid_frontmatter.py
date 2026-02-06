import yaml
def has_valid_frontmatter(content: str) -> bool:
    """Check if content has valid YAML frontmatter."""
    if not content.startswith('---'):
        return False
    parts = content.split('---', 2)
    if len(parts) < 3:
        return False
    try:
        yaml.safe_load(parts[1])
        return True
    except Exception:
        return False
