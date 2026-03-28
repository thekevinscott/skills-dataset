"""E2E tests for filter pass-2 backends.

These tests hit real LLM endpoints (Ollama, Claude agent SDK) to verify
the full pipeline works end-to-end. They use tmp_path for DB and content
setup but make real API calls.

Requires:
  - Ollama running at http://tower.tail790bbc.ts.net:11434 with gemma2:27b
  - ANTHROPIC_API_KEY set for claude-agent-sdk tests
"""

import asyncio
import sqlite3
import types
from pathlib import Path

import pytest

from github_skills_dataset.filter.filter import filter_pass1, filter_pass2

VALID_SKILL = """\
---
name: taint-flow-tracer
description: Trace untrusted data from origin to sink across files and layers.
---

# Taint Flow Tracer

## Purpose
Provide deterministic source-to-sink traces for exploitability decisions.

## Inputs
- `code_path`
- `candidate_source`
- `candidate_sink`

## Trace Method
1. Define exact taint origin and classify trust level.
2. Follow variable flow through wrappers and helpers.
3. Record validation and canonicalization points.
4. Confirm sink invocation path is executable.
5. Output verdict: tainted_reachable, tainted_blocked, or path_unknown.
"""

NOT_A_SKILL = """\
---
title: My Blog Post
date: 2024-01-15
tags: [python, tutorial]
---

# How to Use Python Decorators

Decorators are a powerful feature in Python that allows you to modify
the behavior of functions. Here's a quick tutorial on how to use them.

```python
def my_decorator(func):
    def wrapper():
        print("Before")
        func()
        print("After")
    return wrapper
```

This is just a blog post, not a Claude Code skill.
"""


def _make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


@pytest.fixture
def env(tmp_path):
    """Set up a test environment with source DB, output DB, and content on disk."""
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    content_dir = tmp_path / "content"

    urls = {
        "skill": (_make_url("test", "valid-skill"), VALID_SKILL),
        "not_skill": (_make_url("test", "blog-post"), NOT_A_SKILL),
    }

    # Source DB
    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
    for url, _ in urls.values():
        conn.execute("INSERT INTO files (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

    # Content files on disk
    for url, content in urls.values():
        parts = url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        fp = content_dir / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content)

    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        urls={k: v[0] for k, v in urls.items()},
    )


def _db_rows(output_db):
    """Return {url: (has_frontmatter, is_skill, reason)} for all rows."""
    conn = sqlite3.connect(output_db)
    rows = conn.execute(
        "SELECT url, has_frontmatter, is_skill, reason FROM validation_results"
    ).fetchall()
    conn.close()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _run_pass2(env, backend, **kwargs):
    """Run pass 1 then pass 2 with the given backend."""
    args = types.SimpleNamespace(
        main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
        concurrency=1, limit=None, **kwargs,
    )
    asyncio.run(filter_pass1(args))
    args.backend = backend
    asyncio.run(filter_pass2(args))
    return _db_rows(env.output_db)


OLLAMA_URL = "http://tower.tail790bbc.ts.net:11434/v1"
OLLAMA_MODEL = "gemma2:27b"


@pytest.mark.e2e
class TestOllamaBackend:
    def test_classifies_skill_and_non_skill(self, env):
        """Ollama via OpenAI backend should classify both files without errors."""
        state = _run_pass2(
            env, backend="openai",
            model=OLLAMA_MODEL, base_url=OLLAMA_URL,
        )

        skill_row = state[env.urls["skill"]]
        not_skill_row = state[env.urls["not_skill"]]

        # Both should have been classified (is_skill is not None, no Error in reason)
        assert skill_row[1] is not None, f"skill not classified: {skill_row}"
        assert not_skill_row[1] is not None, f"not_skill not classified: {not_skill_row}"
        assert not skill_row[2] or "Error" not in (skill_row[2] or ""), f"skill errored: {skill_row}"
        assert not not_skill_row[2] or "Error" not in (not_skill_row[2] or ""), f"not_skill errored: {not_skill_row}"

        # The skill should be classified as a skill
        assert skill_row[1] == 1, f"Expected valid skill, got: {skill_row}"

    def test_non_skill_rejected(self, env):
        """Blog post with frontmatter should be rejected."""
        state = _run_pass2(
            env, backend="openai",
            model=OLLAMA_MODEL, base_url=OLLAMA_URL,
        )

        not_skill_row = state[env.urls["not_skill"]]
        assert not_skill_row[1] == 0, f"Expected rejected, got: {not_skill_row}"


@pytest.mark.e2e
class TestAgentSDKBackend:
    def test_classifies_skill_and_non_skill(self, env):
        """Claude agent SDK should classify both files without errors."""
        state = _run_pass2(
            env, backend="claude-agent-sdk",
            model=None, base_url=None,
        )

        skill_row = state[env.urls["skill"]]
        not_skill_row = state[env.urls["not_skill"]]

        assert skill_row[1] is not None, f"skill not classified: {skill_row}"
        assert not_skill_row[1] is not None, f"not_skill not classified: {not_skill_row}"
        assert not skill_row[2] or "Error" not in (skill_row[2] or ""), f"skill errored: {skill_row}"

        assert skill_row[1] == 1, f"Expected valid skill, got: {skill_row}"

    def test_non_skill_rejected(self, env):
        """Blog post with frontmatter should be rejected."""
        state = _run_pass2(
            env, backend="claude-agent-sdk",
            model=None, base_url=None,
        )

        not_skill_row = state[env.urls["not_skill"]]
        assert not_skill_row[1] == 0, f"Expected rejected, got: {not_skill_row}"
