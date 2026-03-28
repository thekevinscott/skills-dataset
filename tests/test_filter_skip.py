"""Unit tests for filter pass 1 and pass 2 skip logic.

Unit tests mock everything except the function under test.
"""

import asyncio
import sqlite3
import types
from pathlib import Path
from unittest import mock

import pytest

from github_skills_dataset.filter.filter import (
    filter_pass1,
    filter_pass2,
    init_output_db,
    scan_content,
)


def make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


URL_FM_YES = make_url("org", "repo1")
URL_FM_NO = make_url("org", "repo2")
URL_NEW = make_url("org", "repo3")

VALID_SKILL = "---\ntitle: Test Skill\n---\n# Hello\nSome content here."
NO_FRONTMATTER = "# Just a heading\nNo YAML here."


# -- Fixtures --

@pytest.fixture
def db_paths(tmp_path):
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    return main_db, output_db


@pytest.fixture
def content_dir(tmp_path):
    d = tmp_path / "content"
    for url, content in [
        (URL_FM_YES, VALID_SKILL),
        (URL_FM_NO, NO_FRONTMATTER),
        (URL_NEW, VALID_SKILL),
    ]:
        parts = url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        fp = d / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content)
    return d


@pytest.fixture
def source_db(db_paths):
    main_db, _ = db_paths
    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO files (url) VALUES (?)", [
        (URL_FM_YES,), (URL_FM_NO,), (URL_NEW,),
    ])
    conn.commit()
    conn.close()
    return main_db


def _args(main_db, output_db, content_dir, **kw):
    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        model="test-model", base_url="http://localhost:1234/v1",
        concurrency=1, backend="anthropic", **kw,
    )


def _db_rows(output_db, where="1=1"):
    conn = sqlite3.connect(output_db)
    rows = conn.execute(f"SELECT url, has_frontmatter, is_skill, reason FROM validation_results WHERE {where}").fetchall()
    conn.close()
    return rows


# -- Unit tests: filter_pass1 --

class TestPass1Unit:
    """Unit tests for filter_pass1. Mocks: scan_content, has_valid_frontmatter, file I/O."""

    def test_skips_already_checked_urls(self, db_paths):
        """URLs with has_frontmatter set should not trigger file reads."""
        main_db, output_db = db_paths

        # Pre-populate DB
        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 0, 0, 'No valid YAML frontmatter')",
            (URL_FM_NO,),
        )
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, 1, 'Valid skill')",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_NO, URL_FM_YES, URL_NEW], 0)), \
             mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db, Path("/fake"))
            asyncio.run(filter_pass1(args))

        # Only URL_NEW should have been read
        assert mock_read.call_count == 1

    def test_persists_frontmatter_failures(self, db_paths):
        """Files without frontmatter get stored with has_frontmatter=0."""
        main_db, output_db = db_paths

        def fake_read(self, *a, **kw):
            if "repo2" in str(self):
                return NO_FRONTMATTER
            return VALID_SKILL

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_NO, URL_FM_YES], 0)), \
             mock.patch.object(Path, "read_text", fake_read):
            args = _args(main_db, output_db, Path("/fake"))
            asyncio.run(filter_pass1(args))

        rows = _db_rows(output_db, "has_frontmatter = 0")
        assert len(rows) == 1
        assert rows[0][0] == URL_FM_NO
        assert rows[0][3] == "No valid YAML frontmatter"

    def test_does_not_write_llm_results(self, db_paths):
        """Pass 1 should not write is_skill=1 for files with frontmatter."""
        main_db, output_db = db_paths

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_YES], 0)), \
             mock.patch.object(Path, "read_text", return_value=VALID_SKILL):
            args = _args(main_db, output_db, Path("/fake"))
            asyncio.run(filter_pass1(args))

        # Files with frontmatter should NOT be in the DB (pass 2's job)
        rows = _db_rows(output_db, "has_frontmatter = 1")
        assert len(rows) == 0


# -- Unit tests: filter_pass2 --

class TestPass2Unit:
    """Unit tests for filter_pass2. Mocks: scan_content, file I/O, LLM client."""

    def test_skips_frontmatter_failures(self, db_paths):
        """URLs marked has_frontmatter=0 should be skipped in pass 2."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 0, 0, 'No valid YAML frontmatter')",
            (URL_FM_NO,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_NO], 0)), \
             mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db, Path("/fake"))
            asyncio.run(filter_pass2(args))

        # Should not have read any files -- all skipped
        assert mock_read.call_count == 0

    def test_skips_already_classified(self, db_paths):
        """URLs with successful LLM results should be skipped."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, 1, 'Valid skill')",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_YES], 0)), \
             mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db, Path("/fake"))
            asyncio.run(filter_pass2(args))

        assert mock_read.call_count == 0

    def test_retries_errors(self, db_paths):
        """URLs with Error: reasons should be retried."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, 0, 'Error: 404')",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch("github_skills_dataset.filter.filter.scan_content",
                        return_value=([], [URL_FM_YES], 0)), \
             mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db, Path("/fake"))
            # Will fail on actual LLM call, that's fine
            try:
                asyncio.run(filter_pass2(args))
            except Exception:
                pass

        # Should have read the file (not skipped)
        assert mock_read.call_count == 1
