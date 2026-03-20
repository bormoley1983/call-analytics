from __future__ import annotations

from typing import Any

from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters
from ports.keywords import KeywordMatchStore, KeywordSource, MaterializationStateStore
from ports.reporting import ReportingSource


def _normalize(text: str) -> str:
    return text.casefold().strip()


def _record_texts(record: ReportCallRecord, match_fields: list[str]) -> dict[str, list[str]]:
    selected = set(match_fields)
    return {
        "summary": [record.summary] if "summary" in selected and record.summary else [],
        "key_questions": [item for item in record.key_questions if item] if "key_questions" in selected else [],
        "objections": [item for item in record.objections if item] if "objections" in selected else [],
    }


def _match_keyword(record: ReportCallRecord, keyword: KeywordDefinition) -> list[dict[str, str]]:
    matches: list[dict[str, str]] = []
    texts = _record_texts(record, keyword.match_fields)
    normalized_terms = [
        (term, normalized)
        for term in keyword.terms
        for normalized in [_normalize(term)]
        if normalized
    ]

    for field_name, values in texts.items():
        for value in values:
            normalized_value = _normalize(value)
            for term, normalized_term in normalized_terms:
                if normalized_term and normalized_term in normalized_value:
                    matches.append(
                        {
                            "field": field_name,
                            "term": term,
                            "text": value,
                        }
                    )
    return matches


def materialize_call_keywords(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    keyword_store: KeywordMatchStore,
    state_store: MaterializationStateStore | None = None,
) -> dict[str, Any]:
    keywords = [keyword for keyword in keyword_source.list_keywords() if keyword.is_active and keyword.terms]
    processed_calls = 0
    matched_calls = 0
    stored_rows = 0

    for record in reporting_source.iter_call_records(ReportFilters()):
        processed_calls += 1
        materialized_rows: list[dict[str, Any]] = []
        for keyword in keywords:
            matches = _match_keyword(record, keyword)
            if not matches:
                continue
            materialized_rows.append(
                {
                    "keyword_id": keyword.keyword_id,
                    "match_count": len(matches),
                    "matched_fields": sorted({match["field"] for match in matches}),
                    "matched_terms": sorted({match["term"] for match in matches}),
                }
            )
        if materialized_rows:
            matched_calls += 1
            stored_rows += len(materialized_rows)
        keyword_store.replace_call_keyword_matches(record.call_id, materialized_rows)

    if state_store is not None:
        state_store.mark_materialization_completed(processed_calls, matched_calls, stored_rows)

    return {
        "processed_calls": processed_calls,
        "matched_calls": matched_calls,
        "stored_rows": stored_rows,
        "active_keywords": len(keywords),
    }
