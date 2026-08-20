"""Shared record-filtering and text-extraction helpers for core reporting.

Previously `_include_record`, `_normalize`, and `_record_texts` were copy-pasted
across `reporting_service.py`, `keywords_service.py`, and `keywords_materialize.py`.
This module is the single source of truth; the old private copies are re-exports
so existing call sites keep working.
"""

from __future__ import annotations

from domain.reporting import ReportCallRecord, ReportFilters


def normalize_text(text: str) -> str:
    """Casefold + strip — the canonical term/text normalization."""
    return text.casefold().strip()


def record_texts(
    record: ReportCallRecord, match_fields: set[str] | list[str]
) -> dict[str, list[str]]:
    """Extract the reportable text fields selected by `match_fields`.

    Returns a mapping of field_name → list of raw text values (empty list when
    the field is not selected or has no content).
    """
    selected = set(match_fields)
    return {
        "summary": [record.summary] if "summary" in selected and record.summary else [],
        "key_questions": (
            [item for item in record.key_questions if item]
            if "key_questions" in selected
            else []
        ),
        "objections": (
            [item for item in record.objections if item]
            if "objections" in selected
            else []
        ),
    }


def include_record(
    record: ReportCallRecord, filters: ReportFilters, spam_threshold: float
) -> bool:
    """Apply ReportFilters + spam/effective flags to a single record."""
    if not filters.matches_record(record):
        return False
    if filters.spam_only and record.spam_probability < spam_threshold:
        return False
    return not (filters.effective_only and not record.effective_call)
