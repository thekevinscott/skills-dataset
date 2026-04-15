"""Tests for export command."""

import sqlite3
from pathlib import Path

import pytest

from github_skills_dataset.export import load_valid_urls
from github_skills_dataset.filter.filter import init_output_db


@pytest.fixture
def validation_db(tmp_path):
    db = tmp_path / "validated.db"
    init_output_db(db)
    conn = sqlite3.connect(db)
    # Ensure file_history table exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS file_history (url TEXT PRIMARY KEY, commits TEXT, fetched_at TEXT)"
    )
    # Skills (with history)
    for i in range(10):
        url = f"https://github.com/org/skill-{i}/blob/main/SKILL.md"
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, classifier_is_skill, classifier_confidence) VALUES (?, 1, 1, ?)",
            (url, 0.9)
        )
        conn.execute(
            "INSERT INTO file_history (url, commits, fetched_at) VALUES (?, '[]', '2026-04-15')",
            (url,)
        )
    # Rejects
    for i in range(5):
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, classifier_is_skill, classifier_confidence) VALUES (?, 1, 0, ?)",
            (f"https://github.com/org/reject-{i}/blob/main/SKILL.md", 0.8)
        )
    # Heuristic reject (should always be excluded)
    conn.execute(
        "INSERT INTO validation_results (url, has_frontmatter, heuristic_reject, classifier_is_skill, classifier_confidence) VALUES (?, 1, 1, 1, 0.9)",
        ("https://github.com/org/heuristic-reject/blob/main/SKILL.md",)
    )
    # No frontmatter (should always be excluded)
    conn.execute(
        "INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 0)",
        ("https://github.com/org/no-fm/blob/main/SKILL.md",)
    )
    # Skill WITHOUT history (should be excluded)
    conn.execute(
        "INSERT INTO validation_results (url, has_frontmatter, classifier_is_skill, classifier_confidence) VALUES (?, 1, 1, ?)",
        ("https://github.com/org/no-history/blob/main/SKILL.md", 0.95)
    )
    conn.commit()
    conn.close()
    return db


class TestLoadValidUrls:
    def test_returns_all_skills(self, validation_db):
        df = load_valid_urls(validation_db)
        assert len(df) == 10

    def test_excludes_heuristic_rejects(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert "https://github.com/org/heuristic-reject/blob/main/SKILL.md" not in urls

    def test_excludes_no_frontmatter(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert "https://github.com/org/no-fm/blob/main/SKILL.md" not in urls

    def test_excludes_rejects(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert not any("reject" in u for u in urls if "heuristic" not in u)

    def test_returns_url_column(self, validation_db):
        df = load_valid_urls(validation_db)
        assert "url" in df.columns
        assert len(df.columns) == 1  # only url, no confidence

    def test_excludes_files_without_history(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert "https://github.com/org/no-history/blob/main/SKILL.md" not in urls
