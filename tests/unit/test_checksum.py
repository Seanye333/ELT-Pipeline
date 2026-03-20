"""Unit tests for checksum utilities."""
from __future__ import annotations

from src.utils.checksum import md5_bytes, sha256_bytes


def test_md5_bytes_deterministic():
    data = b"hello world"
    assert md5_bytes(data) == md5_bytes(data)


def test_md5_bytes_different_inputs():
    assert md5_bytes(b"abc") != md5_bytes(b"xyz")


def test_md5_known_value():
    # MD5 of empty bytes is well-known
    assert md5_bytes(b"") == "d41d8cd98f00b204e9800998ecf8427e"


def test_sha256_bytes():
    data = b"test"
    result = sha256_bytes(data)
    assert len(result) == 64  # 256 bits = 64 hex chars
