"""E2E tests comparing local Ollama models against Claude ground truth.

Runs each model against 16 real SKILL.md files (10 valid, 6 rejected as
classified by Claude) and reports agreement rates.

Requires:
  - Ollama running at OLLAMA_URL with all tested models pulled
  - ANTHROPIC_API_KEY set for claude-agent-sdk baseline
"""

import asyncio
import json
import sqlite3
import types
from pathlib import Path

import pytest

from github_skills_dataset.filter.filter import (
    filter_pass1,
    filter_pass2,
    init_output_db,
)
from github_skills_dataset.filter.parse_github_url import parse_github_url

from .fixtures import FIXTURES

OLLAMA_URL = "http://tower.tail790bbc.ts.net:11434/v1"

OLLAMA_MODELS = [
    "gemma2:27b",
    "qwen2.5:14b",
    "phi4:14b",
    "llama3.1:8b",
    "deepseek-r1:32b",
    "qwen2.5:32b",
    "command-r:35b",
]


def _make_url(idx):
    return f"https://github.com/test/fixture-{idx}/blob/main/SKILL.md"


def _setup_env(tmp_path, fixtures):
    """Create DB and content on disk for a list of (content, expected) fixtures."""
    main_db = tmp_path / "skills.db"
    output_db = tmp_path / "validated.db"
    content_dir = tmp_path / "content"

    conn = sqlite3.connect(main_db)
    conn.execute("CREATE TABLE files (url TEXT PRIMARY KEY)")

    urls = []
    for i, (content, expected) in enumerate(fixtures):
        url = _make_url(i)
        urls.append(url)
        conn.execute("INSERT INTO files (url) VALUES (?)", (url,))

        parsed = parse_github_url(url)
        owner, repo, ref, path = parsed
        fp = content_dir / owner / repo / "blob" / ref / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content)

    conn.commit()
    conn.close()

    return types.SimpleNamespace(
        main_db=main_db, output_db=output_db, content_dir=content_dir,
        urls=urls,
    )


def _classify(env, backend, model=None, base_url=None):
    """Run pass 1 + pass 2, return {url: is_skill} dict."""
    args = types.SimpleNamespace(
        main_db=env.main_db, output_db=env.output_db, content_dir=env.content_dir,
        model=model, base_url=base_url, concurrency=2, backend=backend, limit=None,
    )
    asyncio.run(filter_pass1(args))
    asyncio.run(filter_pass2(args))

    conn = sqlite3.connect(env.output_db)
    rows = conn.execute(
        "SELECT url, is_skill, reason FROM llm_skill_evaluation"
    ).fetchall()
    conn.close()
    return {r[0]: (r[1], r[2]) for r in rows}


def _score(results, fixtures, urls):
    """Compare model results against ground truth. Returns (correct, total, details)."""
    correct = 0
    total = len(fixtures)
    details = []
    for i, (content, expected) in enumerate(fixtures):
        url = urls[i]
        if url not in results:
            details.append({"idx": i, "expected": expected, "got": None, "match": False, "reason": "not classified"})
            continue
        got_skill, reason = results[url]
        got = bool(got_skill)
        match = got == expected
        if match:
            correct += 1
        details.append({
            "idx": i, "expected": expected, "got": got, "match": match,
            "reason": (reason or "")[:80],
        })
    return correct, total, details


@pytest.mark.e2e
class TestModelComparison:
    """Run all models and report agreement with Claude ground truth."""

    def test_claude_agent_sdk_baseline(self, tmp_path):
        """Claude agent SDK should match its own ground truth labels."""
        env = _setup_env(tmp_path, FIXTURES)
        results = _classify(env, backend="claude-agent-sdk")
        correct, total, details = _score(results, FIXTURES, env.urls)

        for d in details:
            if not d["match"]:
                label = "VALID" if d["expected"] else "REJECT"
                got_label = "VALID" if d["got"] else "REJECT"
                print(f"  MISMATCH [{label}->{ got_label}] fixture {d['idx']}: {d['reason']}")

        # Claude should agree with itself on most (allow some variance from non-determinism)
        assert correct >= total - 2, f"Claude baseline: {correct}/{total} match"

    @pytest.mark.parametrize("model", OLLAMA_MODELS)
    def test_ollama_model(self, tmp_path, model):
        """Test an Ollama model against Claude ground truth and report accuracy."""
        env = _setup_env(tmp_path, FIXTURES)
        results = _classify(env, backend="openai", model=model, base_url=OLLAMA_URL)
        correct, total, details = _score(results, FIXTURES, env.urls)

        mismatches = [d for d in details if not d["match"]]
        print(f"\n{'='*60}")
        print(f"MODEL: {model}")
        print(f"Agreement with Claude: {correct}/{total} ({100*correct/total:.0f}%)")
        if mismatches:
            print(f"Mismatches:")
            for d in mismatches:
                label = "VALID" if d["expected"] else "REJECT"
                got_label = "VALID" if d["got"] else "REJECT"
                print(f"  [{label}->{got_label}] fixture {d['idx']}: {d['reason']}")
        print(f"{'='*60}")

        # Write results to /tmp for post-hoc analysis
        report_path = Path(f"/tmp/e2e_model_{model.replace(':', '_').replace('/', '_')}.json")
        report_path.write_text(json.dumps({
            "model": model,
            "correct": correct,
            "total": total,
            "accuracy": correct / total,
            "details": details,
        }, indent=2))
