from __future__ import annotations

from typing import Any

from core.report_filters import normalize_text as _normalize  # re-export
from core.report_filters import record_texts as _record_texts  # re-export
from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters
from ports.keywords import KeywordMatchStore, KeywordSource, MaterializationStateStore
from ports.reporting import ReportingSource


class _TermIndex:
    """Pre-computed reverse index: field → normalized_term → (keyword_id, original_term).

    Replaces the O(n*m) per-call keyword scan with a single pass over each call's
    text fields. Instead of iterating keywords × terms for every call, we iterate
    each text field once and check which indexed terms appear in it.

    This reduces redundant normalizations and leverages Python's fast 'in' operator
    on the (typically longer) text rather than the (shorter) terms.

    E2: terms are bucketed by field up front, so matching a record no longer
    scans every indexed term with a `field != field_name` skip — each field is
    checked against only its own bucket.
    """

    def __init__(self, keywords: list[KeywordDefinition]):
        # Maps field_name → normalized_term → list of (keyword_id, original_term)
        self._by_field: dict[str, dict[str, list[tuple[str, str]]]] = {}
        self._fields: set[str] = set()

        for kw in keywords:
            for field in kw.match_fields:
                self._fields.add(field)
                bucket = self._by_field.setdefault(field, {})
                for term in kw.terms:
                    norm = _normalize(term)
                    if not norm:
                        continue
                    bucket.setdefault(norm, []).append((kw.keyword_id, term))

    @property
    def fields(self) -> set[str]:
        return self._fields

    def matches(self, texts: dict[str, list[str]]) -> list[dict[str, Any]]:
        """Match pre-extracted record texts against the index.

        `texts` maps field_name → list of raw text values (see `_record_texts`).
        Returns materialized rows grouped by keyword_id with deterministic
        ordering. Public so callers/tests can reuse the matching logic without
        reaching into private state.
        """
        # Accumulate matches per keyword: kw_id → {fields, terms, count}
        kw_matches: dict[str, dict[str, Any]] = {}

        for field_name, values in texts.items():
            bucket = self._by_field.get(field_name)
            if not bucket:
                continue
            for value in values:
                normalized_value = _normalize(value)
                if not normalized_value:
                    continue
                for norm_term, kw_list in bucket.items():
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
    return index.matches(texts)


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

    # Use batch writes when available (Postgres) to reduce round-trips from N to ~1.
    # Each batch commits a chunk of calls in a single transaction.
    _batch_fn = getattr(keyword_store, "batch_replace_call_keyword_matches", None)
    _batch: list[tuple[str, list[dict]]] = []
    _batch_size = 500

    def _flush_batch():
        if not _batch:
            return
        if _batch_fn is not None:
            _batch_fn(_batch)
        else:
            for call_id, rows in _batch:
                keyword_store.replace_call_keyword_matches(call_id, rows)
        _batch.clear()

    for record in reporting_source.iter_call_records(ReportFilters()):
        processed_calls += 1
        materialized_rows = _match_record(record, index)

        if materialized_rows:
            matched_calls += 1
            stored_rows += len(materialized_rows)

        _batch.append((record.call_id, materialized_rows))
        if len(_batch) >= _batch_size:
            _flush_batch()

    _flush_batch()

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
