"""Tests for generate-training-data (LLM labeling with CSV as source of truth)."""

import asyncio
import csv
import hashlib
import sqlite3
import types
from pathlib import Path
from unittest import mock

import pytest

from github_skills_dataset.filter.filter import init_output_db, filter_pass1


def make_url(owner, repo, ref="main", path="SKILL.md"):
    return f"https://github.com/{owner}/{repo}/blob/{ref}/{path}"


SKILL_A = "---\nname: skill-a\ndescription: Skill A\n---\n\n# Skill A\n\n## Steps\n1. Do A"
SKILL_B = "---\nname: skill-b\ndescription: Skill B\n---\n\n# Skill B\n\n## Steps\n1. Do B"
SKILL_C = "---\nname: skill-c\ndescription: Skill C\n---\n\n# Skill C\n\n## Steps\n1. Do C"


def content_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.fixture
def env(tmp_path):
    """Environment with 3 skill files, 1 already labeled in CSV."""
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    content_dir = tmp_path / "content"
    csv_path = tmp_path / "labeled.csv"

    examples = [
        (make_url("org", "repo-a"), SKILL_A),
        (make_url("org", "repo-b"), SKILL_B),
        (make_url("org", "repo-c"), SKILL_C),
    ]

    # Source DB
    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")
    for url, _ in examples:
        conn.execute("INSERT INTO files (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

    # Content on disk
    for url, content in examples:
        parts = url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        fp = content_dir / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content)

    # Run pass 1 to populate validation_results
    asyncio.run(filter_pass1(types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
    )))

    # CSV with skill-a already labeled
    with open(csv_path, "w", newline="") as f:
        f.write("# backend=test, model=test\n")
        writer = csv.writer(f)
        writer.writerow(["content_hash", "is_skill"])
        writer.writerow([content_hash(SKILL_A), "true"])

    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        csv_path=csv_path, examples=examples,
    )


def _mock_anthropic_client():
    """Mock that classifies everything as a skill."""
    async def fake_create(**kwargs):
        result = mock.MagicMock()
        result.content = [mock.MagicMock(text='{"is_skill": true}')]
        return result

    client = mock.MagicMock()
    client.messages.create = mock.AsyncMock(side_effect=fake_create)
    return client


class TestGenerateTrainingData:
    def test_skips_already_labeled(self, env):
        """Content hashes already in CSV should not be sent to the LLM."""
        from github_skills_dataset.filter.training import generate_training

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=None,
            )))

        # skill-a was already labeled -- should not have been sent to LLM
        # skill-b and skill-c should have been sent (2 unique content hashes)
        assert client.messages.create.call_count == 2

    def test_appends_to_csv(self, env):
        """New results should be appended to the CSV."""
        from github_skills_dataset.filter.training import generate_training

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=None,
            )))

        # CSV should now have 3 rows (1 original + 2 new)
        with open(env.csv_path) as f:
            lines = [l for l in f if not l.startswith("#")]
        reader = csv.DictReader(lines)
        rows = list(reader)
        assert len(rows) == 3

        hashes = {r["content_hash"] for r in rows}
        assert content_hash(SKILL_A) in hashes
        assert content_hash(SKILL_B) in hashes
        assert content_hash(SKILL_C) in hashes

    def test_respects_limit(self, env):
        """--limit should cap the number of LLM calls."""
        from github_skills_dataset.filter.training import generate_training

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=1,
            )))

        # Only 1 LLM call despite 2 unlabeled
        assert client.messages.create.call_count == 1

        # CSV should have 2 rows (1 original + 1 new)
        with open(env.csv_path) as f:
            lines = [l for l in f if not l.startswith("#")]
        rows = list(csv.DictReader(lines))
        assert len(rows) == 2

    def test_preserves_existing_csv_content(self, env):
        """Existing CSV rows should not be modified."""
        from github_skills_dataset.filter.training import generate_training

        # Read original
        with open(env.csv_path) as f:
            original = f.read()

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=None,
            )))

        # New CSV should start with the original content
        with open(env.csv_path) as f:
            updated = f.read()
        assert updated.startswith(original.rstrip("\n"))

    def test_writes_each_row_immediately(self, env):
        """A run killed mid-flight must keep every already-completed label.

        Call 1 succeeds, call 2 raises a BaseException (simulating a hard
        crash; KeyboardInterrupt itself can't be used -- asyncio tears the
        loop down before pending results are consumed). The first label must
        already be on disk when the crash propagates.
        """
        from github_skills_dataset.filter.training import generate_training

        class SimulatedCrash(BaseException):
            pass

        calls = [0]

        async def fake_create(**kwargs):
            calls[0] += 1
            if calls[0] >= 2:
                raise SimulatedCrash()
            result = mock.MagicMock()
            result.content = [mock.MagicMock(text='{"is_skill": true}')]
            return result

        client = mock.MagicMock()
        client.messages.create = mock.AsyncMock(side_effect=fake_create)

        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            with pytest.raises(SimulatedCrash):
                asyncio.run(generate_training(types.SimpleNamespace(
                    main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                    labeled_csv=env.csv_path, model=None, base_url=None,
                    concurrency=1, backend="anthropic", limit=None,
                )))

        # The completed label (call 1) must have been flushed before the crash
        with open(env.csv_path) as f:
            lines = [l for l in f if not l.startswith("#")]
        rows = list(csv.DictReader(lines))
        labeled = {r["content_hash"] for r in rows}
        # 1 pre-existing + exactly 1 banked from the interrupted run
        assert len(rows) == 2, "completed labels were lost on interrupt"
        assert content_hash(SKILL_B) in labeled or content_hash(SKILL_C) in labeled

    def test_every_run_stamps_provenance_header(self, env):
        """Each run must append its own '# backend=, model=' line, even when
        the CSV already exists -- otherwise mixed-model batches are unauditable."""
        from github_skills_dataset.filter.training import generate_training
        from github_skills_dataset.filter.config import DEFAULT_MODEL

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=None,
            )))

        with open(env.csv_path) as f:
            content = f.read()
        assert f"# backend=anthropic, model={DEFAULT_MODEL}" in content

    def test_deduplicates_by_content_hash(self, env):
        """If multiple URLs have the same content, only one LLM call should be made."""
        from github_skills_dataset.filter.training import generate_training

        # Add a duplicate URL with same content as skill-b
        conn = sqlite3.connect(env.main_db)
        dup_url = make_url("org", "repo-b-copy")
        conn.execute("INSERT INTO files (url) VALUES (?)", (dup_url,))
        conn.commit()
        conn.close()

        parts = dup_url.split("/")
        owner, repo, ref, path = parts[3], parts[4], parts[6], "/".join(parts[7:])
        fp = env.content_dir / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(SKILL_B)  # same content

        # Re-run pass 1 to pick up new URL
        asyncio.run(filter_pass1(types.SimpleNamespace(
            main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
        )))

        client = _mock_anthropic_client()
        with mock.patch("anthropic.AsyncAnthropic", return_value=client):
            asyncio.run(generate_training(types.SimpleNamespace(
                main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
                labeled_csv=env.csv_path, model=None, base_url=None,
                concurrency=1, backend="anthropic", limit=None,
            )))

        # Still only 2 LLM calls (skill-b and skill-c), not 3
        assert client.messages.create.call_count == 2
