from .config import VALIDATION_PROMPT
import hashlib

def prompt_hash(content: str) -> str:
    """Hash the full formatted prompt for cache keying."""
    full_prompt = VALIDATION_PROMPT.format(content=content)
    return hashlib.sha256(full_prompt.encode()).hexdigest()
