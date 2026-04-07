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
    # Skills with various confidence levels
    for i in range(10):
        conn.execute(
            "INSERT INTO validation_results (url, has_frontmatter, classifier_is_skill, classifier_confidence) VALUES (?, 1, 1, ?)",
            (f"https://github.com/org/skill-{i}/blob/main/SKILL.md", 0.9 - i * 0.08)
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
    conn.commit()
    conn.close()
    return db


class TestLoadValidUrls:
    def test_returns_all_skills(self, validation_db):
        df = load_valid_urls(validation_db)
        assert len(df) == 10  # all 10 skills, excludes rejects + heuristic + no-fm

    def test_excludes_heuristic_rejects(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert "https://github.com/org/heuristic-reject/blob/main/SKILL.md" not in urls

    def test_excludes_no_frontmatter(self, validation_db):
        df = load_valid_urls(validation_db)
        urls = df["url"].to_list()
        assert "https://github.com/org/no-fm/blob/main/SKILL.md" not in urls

    def test_min_confidence_filters(self, validation_db):
        df = load_valid_urls(validation_db, min_confidence=0.5)
        # skill-0: 0.9, skill-1: 0.82, ..., skill-6: 0.42 -- first 7 are >= 0.5?
        # 0.9, 0.82, 0.74, 0.66, 0.58, 0.50, 0.42...
        # >= 0.5: indices 0-5 (6 skills)
        assert len(df) == 6

    def test_includes_confidence_column(self, validation_db):
        df = load_valid_urls(validation_db)
        assert "classifier_confidence" in df.columns
