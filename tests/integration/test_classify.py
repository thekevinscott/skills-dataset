"""Tests for SVM classifier (pass 3)."""

import asyncio
import csv
import sqlite3
import types
from pathlib import Path

import pytest

from github_skills_dataset.filter.classify import (
    classify_pass,
    extract_heuristic_features,
    load_labeled_csv,
)
from github_skills_dataset.filter.filter import init_output_db


VALID_SKILL = "---\nname: git-rebase\ndescription: Rebase workflow\n---\n\n# Git Rebase\n\n## When to use\nUse this when rebasing.\n\n## Steps\n1. Fetch\n2. Rebase\n3. Push"
NOT_A_SKILL = "---\ntitle: Blog Post\ndate: 2024-01-15\ncategories: [python]\n---\n\n# Decorators\nHere is how to use them."


def make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


class TestExtractHeuristicFeatures:
    def test_returns_list_of_floats(self):
        features = extract_heuristic_features(VALID_SKILL, "https://github.com/org/repo/blob/main/SKILL.md")
        assert isinstance(features, list)
        assert all(isinstance(f, (int, float)) for f in features)

    def test_detects_name_field(self):
        features = extract_heuristic_features(VALID_SKILL, "https://github.com/org/repo/blob/main/SKILL.md")
        assert features[0] == 1  # has_name

    def test_detects_missing_name(self):
        features = extract_heuristic_features(NOT_A_SKILL, "https://github.com/org/repo/blob/main/SKILL.md")
        assert features[0] == 0  # no name field

    def test_consistent_length(self):
        f1 = extract_heuristic_features(VALID_SKILL, "http://example.com")
        f2 = extract_heuristic_features(NOT_A_SKILL, "http://example.com")
        assert len(f1) == len(f2)


class TestLoadLabeledCSV:
    def test_loads_csv(self, tmp_path):
        csv_path = tmp_path / "labeled.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "is_skill"])
            writer.writerow(["https://github.com/org/repo/blob/main/SKILL.md", "true"])
            writer.writerow(["https://github.com/org/repo2/blob/main/SKILL.md", "false"])

        examples = load_labeled_csv(csv_path)
        assert len(examples) == 2
        assert examples[0]["is_skill"] is True
        assert examples[1]["is_skill"] is False


class TestClassifyPass:
    @pytest.fixture
    def env(self, tmp_path):
        """Set up test environment with labeled CSV and content on disk."""
        main_db = tmp_path / "skills.db"
        output_db = tmp_path / "validated.db"
        content_dir = tmp_path / "content"

        # Create enough examples for train/val split (need at least 10)
        skills = [(make_url("org", f"skill-{i}"), VALID_SKILL) for i in range(20)]
        rejects = [(make_url("org", f"reject-{i}"), NOT_A_SKILL) for i in range(20)]
        all_examples = skills + rejects

        # Source DB
        conn = sqlite3.connect(main_db)
        conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
        for url, _ in all_examples:
            conn.execute("INSERT INTO files (url) VALUES (?)", (url,))
        conn.commit()
        conn.close()

        # Content on disk
        for url, content in all_examples:
            parts = url.split("/")
            owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
            fp = content_dir / owner / repo / "blob" / ref / path
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content)

        # Labeled CSV
        csv_path = tmp_path / "labeled.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "is_skill"])
            for url, content in skills:
                writer.writerow([url, "true"])
            for url, content in rejects:
                writer.writerow([url, "false"])

        # Init DB with pass 1 results
        init_output_db(output_db)
        conn = sqlite3.connect(output_db)
        for url, _ in all_examples:
            conn.execute("INSERT INTO validation_results (url, has_frontmatter) VALUES (?, 1)", (url,))
        conn.commit()
        conn.close()

        return types.SimpleNamespace(
            main_db=main_db, output_db=output_db, content_dir=content_dir,
            csv_path=csv_path,
        )

    def test_classifies_all_urls(self, env):
        """Pass 3 should write embedding_is_skill and embedding_confidence for all URLs."""
        asyncio.run(classify_pass(types.SimpleNamespace(
            output_db=env.output_db, content_dir=env.content_dir,
            labeled_csv=env.csv_path, confidence_threshold=None,
        )))

        conn = sqlite3.connect(env.output_db)
        classified = conn.execute(
            "SELECT COUNT(*) FROM validation_results WHERE embedding_is_skill IS NOT NULL"
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM validation_results").fetchone()[0]
        conn.close()

        assert classified == total, f"Expected all {total} classified, got {classified}"

    def test_produces_confidence_scores(self, env):
        """Confidence scores should be between 0 and 1."""
        asyncio.run(classify_pass(types.SimpleNamespace(
            output_db=env.output_db, content_dir=env.content_dir,
            labeled_csv=env.csv_path, confidence_threshold=None,
        )))

        conn = sqlite3.connect(env.output_db)
        rows = conn.execute(
            "SELECT embedding_confidence FROM validation_results WHERE embedding_confidence IS NOT NULL"
        ).fetchall()
        conn.close()

        assert len(rows) > 0
        for (conf,) in rows:
            assert 0 <= conf <= 1, f"Confidence {conf} out of range"

    def test_separates_skills_from_rejects(self, env):
        """With clear skill vs non-skill examples, classifier should separate them."""
        asyncio.run(classify_pass(types.SimpleNamespace(
            output_db=env.output_db, content_dir=env.content_dir,
            labeled_csv=env.csv_path, confidence_threshold=None,
        )))

        conn = sqlite3.connect(env.output_db)
        skill_preds = conn.execute(
            "SELECT COUNT(*) FROM validation_results WHERE url LIKE '%skill-%' AND embedding_is_skill = 1"
        ).fetchone()[0]
        reject_preds = conn.execute(
            "SELECT COUNT(*) FROM validation_results WHERE url LIKE '%reject-%' AND embedding_is_skill = 0"
        ).fetchone()[0]
        conn.close()

        # With such clear examples, expect most to be correct
        assert skill_preds >= 15, f"Expected most skills classified correctly, got {skill_preds}/20"
        assert reject_preds >= 15, f"Expected most rejects classified correctly, got {reject_preds}/20"
