# -*- coding: utf-8 -*-
"""Shared core utilities used across multiple modules."""

from __future__ import annotations

import hashlib
from hashlib import sha256
from pathlib import Path


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file.

    Reads in 1 MiB chunks to avoid loading the entire file into memory.
    """
    h = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def call_id_hash(s: str) -> str:
    """Generate a 12-character hex prefix of SHA-256 for call identification.

    Replaces the legacy SHA-1 based sha12() to use a cryptographically stronger
    hash function while maintaining the same output length (12 hex chars).
    Existing call_ids will differ from the old SHA-1 values — this is intentional
    and should be treated as a migration boundary.
    """
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]
