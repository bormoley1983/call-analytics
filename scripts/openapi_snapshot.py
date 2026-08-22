"""Deterministic OpenAPI snapshot export and drift check.

Usage (from the repository root):

    # Regenerate the committed snapshot after intentional schema changes:
    PYTHONPATH=src .venv/bin/python scripts/openapi_snapshot.py generate

    # Verify the committed snapshot matches the current code (CI / pre-commit):
    PYTHONPATH=src .venv/bin/python scripts/openapi_snapshot.py check

The export is side-effect safe: it imports ``api.app`` and calls ``openapi()``
without starting the server, contacting databases, or initializing external
services.  Output is stable JSON (sorted keys, 2-space indent) with no
timestamps or host-specific paths, so repeated exports are byte-for-byte
identical.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "openapi" / "call-analytics.json"


def _render_spec() -> str:
    """Render the OpenAPI spec as deterministic JSON text."""
    from api.app import app

    spec = app.openapi()
    return json.dumps(spec, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def generate() -> None:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(_render_spec(), encoding="utf-8")
    print(f"Wrote {SNAPSHOT_PATH.relative_to(Path.cwd()) if SNAPSHOT_PATH.is_relative_to(Path.cwd()) else SNAPSHOT_PATH}")


def check() -> int:
    if not SNAPSHOT_PATH.exists():
        print(f"error: snapshot not found at {SNAPSHOT_PATH}; run 'generate' first", file=sys.stderr)
        return 1
    expected = _render_spec()
    actual = SNAPSHOT_PATH.read_text(encoding="utf-8")
    if expected == actual:
        print(f"OK: {SNAPSHOT_PATH} matches the current OpenAPI schema")
        return 0
    print("error: OpenAPI snapshot drift detected", file=sys.stderr)
    print(
        "The committed snapshot no longer matches the code. "
        "If the schema change is intentional, regenerate with:\n"
        f"  PYTHONPATH=src .venv/bin/python {Path(__file__).name} generate\n"
        "and commit the updated file.",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("generate", help="Regenerate openapi/call-analytics.json")
    sub.add_parser("check", help="Exit non-zero if the snapshot has drifted")
    args = parser.parse_args()
    if args.command == "generate":
        generate()
        return 0
    return check()


if __name__ == "__main__":
    raise SystemExit(main())
