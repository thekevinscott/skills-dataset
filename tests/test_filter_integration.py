"""Integration tests for filter pipeline.

Only mocks external dependencies (LLM API calls). Uses real DB, real files,
real frontmatter checking.
"""

import asyncio
import sqlite3
import types
from pathlib import Path
from unittest import mock

import pytest

from github_skills_dataset.filter.filter import (
    filter,
    filter_pass1,
    filter_pass2,
    init_output_db,
)


def make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


VALID_SKILL = "---\ntitle: Test Skill\n---\n# Hello\nSome content here."
NO_FRONTMATTER = "# Just a heading\nNo YAML here."


@pytest.fixture
def env(tmp_path):
    """Set up a complete test environment with source DB, output DB, and content."""
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    content_dir = tmp_path / "content"

    urls = {
        "valid1": (make_url("org", "repo1"), VALID_SKILL),
        "valid2": (make_url("org", "repo2"), VALID_SKILL),
        "no_fm": (make_url("org", "repo3"), NO_FRONTMATTER),
        "no_content": (make_url("org", "repo4"), None),  # No file on disk
    }

    # Source DB
    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
    for key, (url, content) in urls.items():
        conn.execute("INSERT INTO files (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

    # Content files (skip no_content)
    for key, (url, content) in urls.items():
        if content is None:
            continue
        parts = url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        fp = content_dir / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content)

    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        urls={k: v[0] for k, v in urls.items()},
    )


def _args(env, **kw):
    return types.SimpleNamespace(
        main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
        model="test-model", base_url="http://localhost:1234/v1",
        concurrency=1, backend="anthropic", **kw,
    )


def _db_state(output_db):
    """Return {url: (has_frontmatter, is_skill, reason)} for all rows."""
    conn = sqlite3.connect(output_db)
    rows = conn.execute("SELECT url, has_frontmatter, is_skill, reason FROM validation_results").fetchall()
    conn.close()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _mock_anthropic_client(responses):
    """Create a mock Anthropic client that returns canned responses.

    responses: dict mapping content substring to (is_skill, reason).
    """
    async def fake_create(**kwargs):
        content = kwargs["messages"][0]["content"]
        for key, (is_skill, reason) in responses.items():
            if key in content:
                result = mock.MagicMock()
                result.content = [mock.MagicMock(text=f'{{"is_skill": {str(is_skill).lower()}, "reason": "{reason}"}}')]
                return result
        raise ValueError(f"No mock response for content: {content[:100]}")

    client = mock.MagicMock()
    client.messages.create = mock.AsyncMock(side_effect=fake_create)
    return client


class TestPass1Integration:
    def test_identifies_frontmatter(self, env):
        """Pass 1 correctly separates files with/without frontmatter."""
        asyncio.run(filter_pass1(_args(env)))

        state = _db_state(env.output_db)

        # no_fm should be rejected
        assert env.urls["no_fm"] in state
        assert state[env.urls["no_fm"]][0] == 0  # has_frontmatter=0

        # valid files should be in DB with has_frontmatter=1, is_skill=NULL
        assert env.urls["valid1"] in state
        assert state[env.urls["valid1"]][0] == 1
        assert state[env.urls["valid1"]][1] is None  # not yet classified

        # no_content not in DB (no file on disk)
        assert env.urls["no_content"] not in state

    def test_rerun_reads_no_files(self, env):
        """Second run of pass 1 should not re-read any files."""
        asyncio.run(filter_pass1(_args(env)))

        original_read_text = Path.read_text
        reads = []

        def tracking_read(self, *a, **kw):
            reads.append(str(self))
            return original_read_text(self, *a, **kw)

        with mock.patch.object(Path, "read_text", tracking_read):
            asyncio.run(filter_pass1(_args(env)))

        content_reads = [f for f in reads if "content" in f]
        assert len(content_reads) == 0, f"No files should be re-read, got: {content_reads}"


class TestPass2Integration:
    def test_classifies_via_llm(self, env):
        """Pass 2 calls LLM for files with frontmatter and stores results."""
        asyncio.run(filter_pass1(_args(env)))

        responses = {"Test Skill": (True, "Valid skill file")}
        client = _mock_anthropic_client(responses)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(filter_pass2(_args(env)))

        state = _db_state(env.output_db)
        assert state[env.urls["valid1"]][1] == 1  # is_skill=1
        assert state[env.urls["valid2"]][1] == 1
        assert state[env.urls["no_fm"]][0] == 0  # still has_frontmatter=0

    def test_skips_completed_on_rerun(self, env):
        """Re-running pass 2 should not re-call LLM for completed URLs."""
        asyncio.run(filter_pass1(_args(env)))

        responses = {"Test Skill": (True, "Valid skill file")}
        client = _mock_anthropic_client(responses)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(filter_pass2(_args(env)))

        # Second run -- LLM should not be called
        client2 = _mock_anthropic_client({})
        client2.messages.create = mock.AsyncMock(side_effect=Exception("Should not be called"))

        with mock.patch("anthropic.AsyncAnthropic", return_value=client2):
            asyncio.run(filter_pass2(_args(env)))  # Should not raise


class TestFullPipelineIntegration:
    def test_filter_runs_both_passes(self, env):
        """The combined filter command runs pass 1 then pass 2."""
        responses = {"Test Skill": (True, "Valid skill file")}
        client = _mock_anthropic_client(responses)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(filter(_args(env)))

        state = _db_state(env.output_db)
        assert len(state) == 3  # 2 valid + 1 no_fm
        assert state[env.urls["valid1"]][1] == 1
        assert state[env.urls["valid2"]][1] == 1
        assert state[env.urls["no_fm"]][0] == 0

    def test_rerun_after_full_pipeline(self, env):
        """Full re-run should skip everything."""
        responses = {"Test Skill": (True, "Valid skill file")}
        client = _mock_anthropic_client(responses)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(filter(_args(env)))

        # Re-run -- nothing should be called
        client2 = _mock_anthropic_client({})
        client2.messages.create = mock.AsyncMock(side_effect=Exception("Should not be called"))

        with mock.patch("anthropic.AsyncAnthropic", return_value=client2):
            asyncio.run(filter(_args(env)))  # Should not raise

    def test_scan_runs_once_in_combined(self, env):
        """filter() should only scan content once, not twice."""
        responses = {"Test Skill": (True, "Valid skill file")}
        client = _mock_anthropic_client(responses)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client), \
             mock.patch("github_skills_dataset.filter.filter.scan_content", wraps=__import__("github_skills_dataset.filter.filter", fromlist=["scan_content"]).scan_content) as mock_scan:
            asyncio.run(filter(_args(env)))

        assert mock_scan.call_count == 1
