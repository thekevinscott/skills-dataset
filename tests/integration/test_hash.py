"""Tests for content hashing."""

import hashlib
from pathlib import Path

from github_skills_dataset.hash import content_hash, content_hash_file


class TestContentHash:
    def test_returns_sha256_hex(self):
        result = content_hash(b"hello")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_matches_hashlib(self):
        data = b"---\nname: test\n---\n# Test"
        expected = hashlib.sha256(data).hexdigest()
        assert content_hash(data) == expected

    def test_empty_bytes(self):
        result = content_hash(b"")
        assert result == hashlib.sha256(b"").hexdigest()

    def test_deterministic(self):
        data = b"same content"
        assert content_hash(data) == content_hash(data)

    def test_different_content_different_hash(self):
        assert content_hash(b"a") != content_hash(b"b")


class TestContentHashFile:
    def test_hashes_file(self, tmp_path):
        f = tmp_path / "SKILL.md"
        f.write_bytes(b"---\nname: test\n---\n# Test")
        assert content_hash_file(f) == content_hash(b"---\nname: test\n---\n# Test")

    def test_matches_sha256sum(self, tmp_path):
        """Hash should match sha256sum output."""
        content = b"hello world\n"
        f = tmp_path / "test.md"
        f.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert content_hash_file(f) == expected

    def test_binary_content_preserved(self, tmp_path):
        """Raw bytes hashed, no encoding conversion."""
        content = b"\xff\xfe invalid utf-8"
        f = tmp_path / "test.md"
        f.write_bytes(content)
        assert content_hash_file(f) == hashlib.sha256(content).hexdigest()
