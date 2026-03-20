"""MD5 / SHA-256 checksum utilities for idempotency checks."""
from __future__ import annotations

import hashlib
from pathlib import Path


def md5_bytes(data: bytes) -> str:
    """Return hex MD5 of raw bytes."""
    return hashlib.md5(data).hexdigest()


def md5_file(path: str | Path) -> str:
    """Return hex MD5 of a file on disk (chunked for large files)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Return hex SHA-256 of raw bytes."""
    return hashlib.sha256(data).hexdigest()
