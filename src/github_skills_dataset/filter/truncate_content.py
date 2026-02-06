from .config import CONTENT_MAX_BYTES

def truncate_content(content: str, max_bytes: int = CONTENT_MAX_BYTES) -> str:
    """Truncate content to max_bytes, preserving valid UTF-8."""
    encoded = content.encode('utf-8')
    if len(encoded) <= max_bytes:
        return content
    return encoded[:max_bytes].decode('utf-8', errors='ignore') + "\n[truncated]"

