def parse_github_url(url: str) -> tuple[str, str, str, str] | None:
    """Parse GitHub URL into (owner, repo, ref, path)."""
    parts = url.split('/')
    if len(parts) < 8 or parts[2] != 'github.com' or parts[5] != 'blob':
        return None
    return parts[3], parts[4], parts[6], '/'.join(parts[7:])
