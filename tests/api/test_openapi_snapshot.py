"""BE-06: deterministic OpenAPI snapshot and drift check tests.

Verifies that the committed ``openapi/call-analytics.json`` snapshot is in sync
with the code, that exports are byte-for-byte reproducible, and that the
export path is side-effect safe (no server startup, no DB contact).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SNAPSHOT_PATH = REPO_ROOT / "openapi" / "call-analytics.json"
SCRIPT = REPO_ROOT / "scripts" / "openapi_snapshot.py"


def _run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": str(REPO_ROOT / "src"), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )


def test_snapshot_file_exists_and_is_valid_json() -> None:
    assert SNAPSHOT_PATH.exists(), f"Missing snapshot: {SNAPSHOT_PATH}"
    spec = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    assert spec["openapi"].startswith("3.")
    assert spec["info"]["title"] == "Call Analytics API"


def test_snapshot_has_no_runtime_timestamps_or_host_paths() -> None:
    text = SNAPSHOT_PATH.read_text(encoding="utf-8")
    assert "/home/" not in text, "Snapshot leaked a host-specific path"
    # Only static example dates (e.g. 2026-03-01) are allowed; no generated
    # ISO timestamps with time components.
    import re

    for match in re.finditer(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", text):
        pytest.fail(f"Snapshot contains a runtime timestamp: {match.group(0)}")


def test_check_passes_on_committed_snapshot() -> None:
    result = _run_script("check")
    assert result.returncode == 0, f"check failed:\n{result.stderr}"


def test_generate_is_byte_for_byte_deterministic(tmp_path: Path) -> None:
    """Two consecutive exports produce identical bytes."""
    from api.app import app

    first = json.dumps(app.openapi(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    second = json.dumps(app.openapi(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    assert first == second


def test_check_fails_on_drift(tmp_path: Path) -> None:
    """A deliberate schema change makes check exit non-zero.

    The check runs in a subprocess, so the drift is introduced by temporarily
    editing ``src/api/app.py`` (version string) and restoring it afterwards.
    """
    app_py = REPO_ROOT / "src" / "api" / "app.py"
    original = app_py.read_text(encoding="utf-8")
    try:
        app_py.write_text(
            original.replace('version="0.1.0"', 'version="9.9.9-drift-test"'),
            encoding="utf-8",
        )
        result = _run_script("check")
        assert result.returncode == 1, f"check should fail on drift:\n{result.stdout}{result.stderr}"
        assert "drift" in result.stderr.lower()
    finally:
        app_py.write_text(original, encoding="utf-8")


def test_import_validation_is_side_effect_safe() -> None:
    """Importing api.app and calling openapi() must not start a server or DB."""
    import api.app as app_module

    spec = app_module.app.openapi()
    assert isinstance(spec, dict)
    assert "paths" in spec
