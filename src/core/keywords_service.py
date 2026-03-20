from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters
from ports.keywords import KeywordSource
from ports.reporting import ReportingSource


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _include_record(record: ReportCallRecord, filters: ReportFilters, spam_threshold: float) -> bool:
    if not filters.matches_record(record):
        return False
    if filters.spam_only and record.spam_probability < spam_threshold:
        return False
    if filters.effective_only and not record.effective_call:
        return False
    return True


def list_keywords(keyword_source: KeywordSource) -> dict[str, Any]:
    keywords = [
        {
            "keyword_id": keyword.keyword_id,
            "label": keyword.label,
            "category": keyword.category,
            "terms": keyword.terms,
            "match_fields": keyword.match_fields,
            "is_active": keyword.is_active,
        }
        for keyword in keyword_source.list_keywords()
    ]
    return {
        "generated_at": _utc_now_iso(),
        "data_source": keyword_source.source_name,
        "total_keywords": len(keywords),
        "keywords": keywords,
    }


def build_keywords_report(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    filters: ReportFilters,
    spam_threshold: float,
    sort_by: str = "matched_calls",
    order: str = "desc",
) -> dict[str, Any]:
    keywords = [keyword for keyword in keyword_source.list_keywords() if keyword.is_active and keyword.terms]
    buckets: dict[str, dict[str, Any]] = {
        keyword.keyword_id: {
            "keyword_id": keyword.keyword_id,
            "label": keyword.label,
            "category": keyword.category,
            "terms": keyword.terms,
            "match_fields": keyword.match_fields,
            "matched_calls": 0,
            "total_matches": 0,
            "matched_managers": set(),
            "intents": {},
            "outcomes": {},
        }
        for keyword in keywords
    }

    for record in reporting_source.iter_call_records(filters):
        if not _include_record(record, filters, spam_threshold):
            continue
        for keyword in keywords:
            matches = _match_keyword(record, keyword)
            if not matches:
                continue
            bucket = buckets[keyword.keyword_id]
            bucket["matched_calls"] += 1
            bucket["total_matches"] += len(matches)
            bucket["matched_managers"].add(record.manager_id)
            bucket["intents"][record.intent] = bucket["intents"].get(record.intent, 0) + 1
            bucket["outcomes"][record.outcome] = bucket["outcomes"].get(record.outcome, 0) + 1

    result_keywords = []
    for bucket in buckets.values():
        result_keywords.append(
            {
                "keyword_id": bucket["keyword_id"],
                "label": bucket["label"],
                "category": bucket["category"],
                "terms": bucket["terms"],
                "match_fields": bucket["match_fields"],
                "matched_calls": bucket["matched_calls"],
                "total_matches": bucket["total_matches"],
                "matched_managers": len(bucket["matched_managers"]),
                "top_intents": sorted(bucket["intents"].items(), key=lambda kv: kv[1], reverse=True)[:10],
                "top_outcomes": sorted(bucket["outcomes"].items(), key=lambda kv: kv[1], reverse=True)[:5],
            }
        )

    reverse = order == "desc"
    if sort_by in {"label", "category"}:
        result_keywords.sort(
            key=lambda item: (item[sort_by], item["label"], item["keyword_id"]),
            reverse=reverse,
        )
    else:
        result_keywords.sort(
            key=lambda item: (item.get(sort_by, 0), item["category"], item["label"], item["keyword_id"]),
            reverse=reverse,
        )

    return {
        "generated_at": _utc_now_iso(),
        "report_data_source": reporting_source.source_name,
        "keyword_data_source": keyword_source.source_name,
        "filters": filters.as_dict(),
        "total_keywords": len(result_keywords),
        "keywords_with_matches": sum(1 for item in result_keywords if item["matched_calls"] > 0),
        "keywords": result_keywords,
    }
