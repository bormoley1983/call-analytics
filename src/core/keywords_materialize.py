from __future__ import annotations

from typing import Any

from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters
from ports.keywords import KeywordMatchStore, KeywordSource, MaterializationStateStore
from ports.reporting import ReportingSource


def _normalize(text: str) -> str:
    return text.casefold().strip()


class _TermIndex:
    """Pre-computed reverse index: normalized_term → (keyword_id, field_name, original_term).

    Replaces the O(n*m) per-call keyword scan with a single pass over each call's
    text fields. Instead of iterating keywords × terms for every call, we iterate
    each text field once and check which indexed terms appear in it.

    This reduces redundant normalizations and leverages Python's fast 'in' operator
    on the (typically longer) text rather than the (shorter) terms.
    """

    def __init__(self, keywords: list[KeywordDefinition]):
        # Maps (field_name, normalized_term) → list of (keyword_id, original_term)
        self._index: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._fields: set[str] = set()

        for kw in keywords:
            for field in kw.match_fields:
                self._fields.add(field)
                for term in kw.terms:
                    norm = _normalize(term)
                    if not norm:
                        continue
                    key = (field, norm)
                    self._index.setdefault(key, []).append((kw.keyword_id, term))

    @property
    def fields(self) -> set[str]:
        return self._fields


def _record_texts(
    record: ReportCallRecord, match_fields: set[str]
) -> dict[str, list[str]]:
    selected = match_fields
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


def _match_record(
    record: ReportCallRecord,
    index: _TermIndex,
) -> list[dict[str, Any]]:
    """Match a single record against the pre-built term index.

    Returns materialized rows grouped by keyword_id.
    """
    texts = _record_texts(record, index.fields)
    if not texts:
        return []

    # Accumulate matches per keyword: kw_id → {fields, terms, count}
    kw_matches: dict[str, dict[str, Any]] = {}

    for field_name, values in texts.items():
        for value in values:
            normalized_value = _normalize(value)
            if not normalized_value:
                continue
            # Check all terms for this field against the text
            for (field, norm_term), kw_list in index._index.items():
                if field != field_name:
                    continue
                if norm_term in normalized_value:
                    for kw_id, orig_term in kw_list:
                        entry = kw_matches.setdefault(
                            kw_id,
                            {
                                "keyword_id": kw_id,
                                "match_count": 0,
                                "_fields": set(),
                                "_terms": set(),
                            },
                        )
                        entry["match_count"] += 1
                        entry["_fields"].add(field_name)
                        entry["_terms"].add(orig_term)

    # Convert sets to sorted lists for deterministic output
    result: list[dict[str, Any]] = []
    for entry in kw_matches.values():
        result.append(
            {
                "keyword_id": entry["keyword_id"],
                "match_count": entry["match_count"],
                "matched_fields": sorted(entry["_fields"]),
                "matched_terms": sorted(entry["_terms"]),
            }
        )

    return result


def materialize_call_keywords(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    keyword_store: KeywordMatchStore,
    state_store: MaterializationStateStore | None = None,
) -> dict[str, Any]:
    keywords = [
        keyword
        for keyword in keyword_source.list_keywords()
        if keyword.is_active and keyword.terms
    ]

    # Build the term index once — avoids re-normalizing terms for every call
    index = _TermIndex(keywords)

    processed_calls = 0
    matched_calls = 0
    stored_rows = 0

    for record in reporting_source.iter_call_records(ReportFilters()):
        processed_calls += 1
        materialized_rows = _match_record(record, index)

        if materialized_rows:
            matched_calls += 1
            stored_rows += len(materialized_rows)

        keyword_store.replace_call_keyword_matches(record.call_id, materialized_rows)

    if state_store is not None:
        state_store.mark_materialization_completed(
            processed_calls, matched_calls, stored_rows
        )

    return {
        "processed_calls": processed_calls,
        "matched_calls": matched_calls,
        "stored_rows": stored_rows,
        "active_keywords": len(keywords),
    }
