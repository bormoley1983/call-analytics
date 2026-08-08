"""Golden baseline normalization and comparison utilities.

Strips non-deterministic fields (timestamps, generated_at) from responses
before comparing against stored baselines. This ensures stable comparisons
across test runs while still validating structure and key values.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# Fields that are inherently non-deterministic across runs
_NON_DETERMINISTIC_FIELDS = {
    "generated_at",  # Timestamp of report generation
}

# Fields under 'freshness' that change per run
_FRESHNESS_VOLATILE_FIELDS = {
    "latest_processed_at",
    "latest_materialized_at",
    "latest_keyword_ai_analysis_at",
    "keyword_ai_analysis_status",
}

# ISO 8601 timestamp pattern for string replacement
_ISO_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:?\d{2}|Z)?",
    re.ASCII,
)

# UUID pattern for generated IDs
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

GOLDEN_DIR = Path(__file__).resolve().parent


def _normalize_value(key: str, value: Any) -> Any:
    """Normalize a single value based on its key name."""
    if key in _NON_DETERMINISTIC_FIELDS:
        return "__TIMESTAMP__"
    if isinstance(value, str) and _ISO_TIMESTAMP_RE.match(value):
        return "__TIMESTAMP__"
    return value


def normalize_report(response: dict[str, Any]) -> dict[str, Any]:
    """Normalize a report response for stable golden comparison.

    Strips:
    - Top-level non-deterministic fields (generated_at)
    - Freshness timestamps (latest_processed_at, etc.)
    - keyword_ai_analysis content (depends on LLM runs)
    - ISO timestamp strings in known fields
    - UUIDs in generated IDs
    """
    normalized: dict[str, Any] = {}
    for key, value in response.items():
        if key in _NON_DETERMINISTIC_FIELDS:
            normalized[key] = "__TIMESTAMP__"
            continue

        # Normalize freshness block
        if key == "freshness" and isinstance(value, dict):
            normalized[key] = _normalize_freshness(value)
            continue

        # Skip LLM-generated content (non-deterministic)
        if key == "keyword_ai_analysis":
            normalized[key] = "__AI_ANALYSIS__"
            continue

        # Normalize nested structures
        if isinstance(value, dict):
            normalized[key] = _normalize_dict(value)
        elif isinstance(value, list):
            normalized[key] = [
                _normalize_dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            normalized[key] = value

    return normalized


def _normalize_freshness(freshness: dict[str, Any]) -> dict[str, Any]:
    """Normalize freshness metadata block."""
    result: dict[str, Any] = {}
    for key, value in freshness.items():
        if key in _FRESHNESS_VOLATILE_FIELDS:
            result[key] = "__FRESHNESS_VALUE__"
        else:
            result[key] = value
    return result


def _normalize_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively normalize a dictionary."""
    result: dict[str, Any] = {}
    for key, value in d.items():
        normalized_val = _normalize_value(key, value)
        if isinstance(normalized_val, dict):
            result[key] = _normalize_dict(normalized_val)
        elif isinstance(normalized_val, list):
            result[key] = [
                _normalize_dict(item) if isinstance(item, dict) else item
                for item in normalized_val
            ]
        else:
            result[key] = normalized_val
    return result


def load_golden(endpoint: str) -> dict[str, Any]:
    """Load a golden baseline file for the given endpoint.

    Args:
        endpoint: Endpoint name like 'overall_report', 'managers_report', etc.

    Returns:
        Parsed JSON as dictionary.

    Raises:
        FileNotFoundError: If the golden file doesn't exist yet.
    """
    golden_path = GOLDEN_DIR / f"{endpoint}.json"
    return json.loads(golden_path.read_text(encoding="utf-8"))


def save_golden(endpoint: str, data: dict[str, Any]) -> None:
    """Save a normalized response as the golden baseline for an endpoint.

    Use this to regenerate baselines when report structure intentionally changes.

    Args:
        endpoint: Endpoint name like 'overall_report', 'managers_report', etc.
        data: The raw (unnormalized) response to save.
    """
    normalized = normalize_report(data)
    golden_path = GOLDEN_DIR / f"{endpoint}.json"
    golden_path.write_text(
        json.dumps(normalized, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def assert_matches_golden(
    endpoint: str,
    actual: dict[str, Any],
) -> None:
    """Assert that a normalized response matches the stored golden baseline.

    This is the main assertion function for golden tests. It normalizes both
    the actual response and the golden baseline, then does a deep equality check.

    Args:
        endpoint: Endpoint name matching the golden file (without .json).
        actual: The raw API response to validate.

    Raises:
        AssertionError: If the normalized responses don't match.
        FileNotFoundError: If no golden baseline exists yet.
    """
    expected = load_golden(endpoint)
    normalized_actual = normalize_report(actual)

    if normalized_actual != expected:
        # Build a detailed diff message
        actual_json = json.dumps(normalized_actual, indent=2, ensure_ascii=False)
        expected_json = json.dumps(expected, indent=2, ensure_ascii=False)

        # Find differences
        all_keys = set(list(normalized_actual.keys()) + list(expected.keys()))
        diffs: list[str] = []
        for key in sorted(all_keys):
            if key not in expected:
                diffs.append(f"  + '{key}': {json.dumps(normalized_actual[key])}")
            elif key not in normalized_actual:
                diffs.append(f"  - '{key}': {json.dumps(expected[key])}")
            elif normalized_actual.get(key) != expected.get(key):
                diffs.append(f"  ~ '{key}' differs:")
                diffs.append(
                    f"    actual:   {json.dumps(normalized_actual.get(key))[:200]}"
                )
                diffs.append(f"    expected: {json.dumps(expected.get(key))[:200]}")

        diff_msg = "\n".join(diffs) if diffs else "(unable to compute diff)"
        raise AssertionError(
            f"Golden mismatch for '{endpoint}':\n{diff_msg}\n\n"
            f"Full actual:\n{actual_json}\n\n"
            f"Full expected:\n{expected_json}"
        )
