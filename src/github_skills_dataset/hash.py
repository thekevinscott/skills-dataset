"""Content hashing for SKILL.md files.

The hash is sha256 of the raw file bytes. No truncation, no encoding
conversion. Reproducible with: sha256sum path/to/SKILL.md
"""

import hashlib
from pathlib import Path


def content_hash(data: bytes) -> str:
    """SHA256 hash of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def content_hash_file(path: Path) -> str:
    """SHA256 hash of a file's raw bytes."""
    return content_hash(path.read_bytes())
