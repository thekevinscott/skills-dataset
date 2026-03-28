"""Test that pass 1 skips URLs already checked for frontmatter."""

import asyncio
import sqlite3
import types
from pathlib import Path

import pytest

from github_skills_dataset.filter.filter import filter, init_output_db


def make_url(owner, repo, ref, path):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


URL_WITH_FRONTMATTER = make_url("org", "repo1", "main", "SKILL.md")
URL_NO_FRONTMATTER = make_url("org", "repo2", "main", "SKILL.md")
URL_NEW = make_url("org", "repo3", "main", "SKILL.md")

VALID_SKILL = "---\ntitle: Test Skill\n---\n# Hello\nSome content here."
NO_FRONTMATTER = "# Just a heading\nNo YAML here."


@pytest.fixture
def setup_dirs(tmp_path):
    """Create source DB, output DB, and content files."""
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    content_dir = tmp_path / "content"

    # Create source DB with 3 URLs
    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO files (url) VALUES (?)", [
        (URL_WITH_FRONTMATTER,),
        (URL_NO_FRONTMATTER,),
        (URL_NEW,),
    ])
    conn.commit()
    conn.close()

    # Create content files for all 3
    for url, content in [
        (URL_WITH_FRONTMATTER, VALID_SKILL),
        (URL_NO_FRONTMATTER, NO_FRONTMATTER),
        (URL_NEW, VALID_SKILL),
    ]:
        parts = url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        file_path = content_dir / owner / repo / "blob" / ref / path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content)

    return main_db, output_db, content_dir


def make_args(main_db, output_db, content_dir, **kwargs):
    args = types.SimpleNamespace(
        main_db=main_db,
        output_db=output_db,
        content_dir=content_dir,
        model="test-model",
        base_url="http://localhost:1234/v1",
        concurrency=1,
        backend="anthropic",
        **kwargs,
    )
    return args


def test_pass1_skips_already_checked_frontmatter(setup_dirs):
    """URLs with has_frontmatter already set in DB should not be re-read from disk."""
    main_db, output_db, content_dir = setup_dirs

    # Pre-populate output DB: mark URL_NO_FRONTMATTER as already checked (no frontmatter)
    init_output_db(output_db)
    conn = sqlite3.connect(output_db)
    conn.execute(
        "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 0, 0, 'No valid YAML frontmatter')",
        (URL_NO_FRONTMATTER,),
    )
    # Mark URL_WITH_FRONTMATTER as already LLM-classified
    conn.execute(
        "INSERT INTO validation_results (url, has_frontmatter, is_skill, reason) VALUES (?, 1, 1, 'Valid skill')",
        (URL_WITH_FRONTMATTER,),
    )
    conn.commit()
    conn.close()

    # Track which files get read
    original_read_text = Path.read_text
    files_read = []

    def tracking_read_text(self, *args, **kwargs):
        files_read.append(str(self))
        return original_read_text(self, *args, **kwargs)

    # Run filter -- should only read URL_NEW's content, not the other two
    args = make_args(main_db, output_db, content_dir)

    import unittest.mock as mock
    with mock.patch.object(Path, 'read_text', tracking_read_text):
        # Will error on LLM call for URL_NEW, that's fine -- we only care about pass 1
        try:
            asyncio.run(filter(args))
        except Exception:
            pass

    # Only URL_NEW should have been read from disk
    content_reads = [f for f in files_read if "content" in f]
    assert len(content_reads) == 1, f"Expected 1 file read, got {len(content_reads)}: {content_reads}"
    assert "repo3" in content_reads[0], f"Expected repo3 file, got {content_reads[0]}"


def test_pass1_persists_frontmatter_failures(setup_dirs):
    """Files without valid frontmatter should be stored in DB with has_frontmatter=0."""
    main_db, output_db, content_dir = setup_dirs
    init_output_db(output_db)

    args = make_args(main_db, output_db, content_dir)
    try:
        asyncio.run(filter(args))
    except Exception:
        pass  # LLM calls will fail, that's fine

    conn = sqlite3.connect(output_db)
    row = conn.execute(
        "SELECT has_frontmatter, is_skill, reason FROM validation_results WHERE url = ?",
        (URL_NO_FRONTMATTER,),
    ).fetchone()
    conn.close()

    assert row is not None, "Frontmatter failure should be in DB"
    assert row[0] == 0, f"has_frontmatter should be 0, got {row[0]}"
    assert row[1] == 0, f"is_skill should be 0, got {row[1]}"
    assert "frontmatter" in row[2].lower(), f"reason should mention frontmatter, got {row[2]}"


def test_rerun_skips_frontmatter_failures(setup_dirs):
    """Second run should skip URLs that failed frontmatter on the first run."""
    main_db, output_db, content_dir = setup_dirs
    init_output_db(output_db)

    args = make_args(main_db, output_db, content_dir)

    # First run -- will check all files
    try:
        asyncio.run(filter(args))
    except Exception:
        pass

    # Second run -- track reads
    original_read_text = Path.read_text
    files_read = []

    def tracking_read_text(self, *args, **kwargs):
        files_read.append(str(self))
        return original_read_text(self, *args, **kwargs)

    import unittest.mock as mock
    with mock.patch.object(Path, 'read_text', tracking_read_text):
        try:
            asyncio.run(filter(args))
        except Exception:
            pass

    content_reads = [f for f in files_read if "content" in f]
    # URL_NO_FRONTMATTER should NOT be read again (already in DB as has_frontmatter=0)
    no_fm_reads = [f for f in content_reads if "repo2" in f]
    assert len(no_fm_reads) == 0, f"Frontmatter failure should not be re-read, but got: {no_fm_reads}"
