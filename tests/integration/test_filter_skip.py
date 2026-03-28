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
)


def make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


URL_FM_YES = make_url("org", "repo1")
URL_FM_NO = make_url("org", "repo2")
URL_NEW = make_url("org", "repo3")

VALID_SKILL = "---\ntitle: Test Skill\n---\n# Hello\nSome content here."
NO_FRONTMATTER = "# Just a heading\nNo YAML here."


@pytest.fixture
def db_paths(tmp_path):
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    return main_db, output_db


def _args(main_db, output_db, **kw):
    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=Path("/fake"),
        model="test-model", base_url="http://localhost:1234/v1",
        concurrency=1, backend="anthropic", **kw,
    )


def _fm_rows(output_db, where="1=1"):
    conn = sqlite3.connect(output_db)
    rows = conn.execute(f"SELECT url, has_frontmatter FROM validation_results WHERE {where}").fetchall()
    conn.close()
    return rows


def _eval_rows(output_db, where="1=1"):
    conn = sqlite3.connect(output_db)
    rows = conn.execute(f"SELECT url, is_skill, reason FROM llm_skill_evaluation WHERE {where}").fetchall()
    conn.close()
    return rows


# -- Unit tests: filter_pass1 --

class TestPass1Unit:
    """Unit tests for filter_pass1. Mocks: scan_content, has_valid_frontmatter, file I/O."""

    def test_skips_already_checked_urls(self, db_paths):
        """URLs in validation_results should not trigger file reads."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 0)",
            (URL_FM_NO,),
        )
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 1)",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass1(args, to_validate=[URL_FM_NO, URL_FM_YES, URL_NEW]))

        # Only URL_NEW should have been read
        assert mock_read.call_count == 1

    def test_persists_frontmatter_failures(self, db_paths):
        """Files without frontmatter get stored with has_frontmatter=0."""
        main_db, output_db = db_paths

        def fake_read(self, *a, **kw):
            if "repo2" in str(self):
                return NO_FRONTMATTER
            return VALID_SKILL

        with mock.patch.object(Path, "read_text", fake_read):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass1(args, to_validate=[URL_FM_NO, URL_FM_YES]))

        rows = _fm_rows(output_db, "has_frontmatter = 0")
        assert len(rows) == 1
        assert rows[0][0] == URL_FM_NO

    def test_persists_frontmatter_successes(self, db_paths):
        """Files with valid frontmatter get stored with has_frontmatter=1."""
        main_db, output_db = db_paths

        with mock.patch.object(Path, "read_text", return_value=VALID_SKILL):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass1(args, to_validate=[URL_FM_YES]))

        rows = _fm_rows(output_db, "has_frontmatter = 1")
        assert len(rows) == 1
        assert rows[0][0] == URL_FM_YES


# -- Unit tests: filter_pass2 --

class TestPass2Unit:
    """Unit tests for filter_pass2. Mocks: scan_content, file I/O, LLM client."""

    def test_skips_frontmatter_failures(self, db_paths):
        """URLs marked has_frontmatter=0 should be skipped in pass 2."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 0)",
            (URL_FM_NO,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass2(args, to_validate=[URL_FM_NO]))

        assert mock_read.call_count == 0

    def test_skips_already_classified(self, db_paths):
        """URLs with successful LLM results should be skipped."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 1)",
            (URL_FM_YES,),
        )
        conn.execute(
            "INSERT INTO llm_skill_evaluation (url, backend, model, is_skill, reason) VALUES (?, 'anthropic', 'test-model', 1, 'Valid skill')",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass2(args, to_validate=[URL_FM_YES]))

        assert mock_read.call_count == 0

    def test_retries_errors(self, db_paths):
        """URLs with Error: reasons should be retried."""
        main_db, output_db = db_paths

        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 1)",
            (URL_FM_YES,),
        )
        conn.execute(
            "INSERT INTO llm_skill_evaluation (url, backend, model, is_skill, reason) VALUES (?, 'anthropic', 'test-model', 0, 'Error: 404')",
            (URL_FM_YES,),
        )
        conn.commit()
        conn.close()

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db)
            try:
                asyncio.run(filter_pass2(args, to_validate=[URL_FM_YES]))
            except Exception:
                pass

        # Should have read the file (not skipped)
        assert mock_read.call_count == 1

    def test_skips_unchecked_urls(self, db_paths):
        """URLs not in validation_results are not in no_frontmatter set, so they proceed."""
        main_db, output_db = db_paths
        init_output_db(output_db)

        mock_read = mock.MagicMock(return_value=VALID_SKILL)

        with mock.patch.object(Path, "read_text", mock_read):
            args = _args(main_db, output_db)
            asyncio.run(filter_pass2(args, to_validate=[URL_FM_YES]))

        # URL not in validation_results -> not in no_frontmatter -> proceeds to prep
        assert mock_read.call_count == 1
