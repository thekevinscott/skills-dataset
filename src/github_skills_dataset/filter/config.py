from pathlib import Path
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
CONTENT_MAX_BYTES = 3000      # Truncate for classification; frontmatter + intro is enough
CACHE_DIR = Path.home() / ".cache/skills-dataset/claude"

with open(Path(__file__).parent / './validation_prompt.txt', 'r') as f:
    VALIDATION_PROMPT = f.read()
