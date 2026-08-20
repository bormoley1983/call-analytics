from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, cast

from core.report_filters import include_record as _include_record  # re-export
from core.report_filters import normalize_text as _normalize  # re-export
from core.report_filters import record_texts as _record_texts  # re-export
from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters
from ports.keywords import KeywordSource
from ports.reporting import ReportingSource, SqlReportingSource

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _match_keyword(
    record: ReportCallRecord, keyword: KeywordDefinition
) -> list[dict[str, str]]:
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
    keywords = [
        keyword
        for keyword in keyword_source.list_keywords()
        if keyword.is_active and keyword.terms
    ]

    if isinstance(reporting_source, SqlReportingSource):
        return _build_keywords_report_sql(
            reporting_source=reporting_source,
            keyword_source=keyword_source,
            keywords=keywords,
            filters=filters,
            spam_threshold=spam_threshold,
            sort_by=sort_by,
            order=order,
        )

    return _build_keywords_report_iterative(
        reporting_source=reporting_source,
        keyword_source=keyword_source,
        keywords=keywords,
        filters=filters,
        spam_threshold=spam_threshold,
        sort_by=sort_by,
        order=order,
    )


def _build_keywords_report_iterative(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    keywords: list[KeywordDefinition],
    filters: ReportFilters,
    spam_threshold: float,
    sort_by: str,
    order: str,
) -> dict[str, Any]:
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

    logger.info("Starting keywords report (iterative): records from %s keywords=%d", reporting_source.source_name, len(keywords))
    records_processed = 0
    last_log_at = 0
    for record in reporting_source.iter_call_records(filters):
        records_processed += 1
        if records_processed - last_log_at >= 5000:
            logger.info("Keywords report progress: records_processed=%d", records_processed)
            last_log_at = records_processed
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
            bucket["intents"][record.intent] = (
                bucket["intents"].get(record.intent, 0) + 1
            )
            bucket["outcomes"][record.outcome] = (
                bucket["outcomes"].get(record.outcome, 0) + 1
            )

    logger.info(
        "Finished keywords report iteration: records_processed=%d",
        records_processed,
    )

    return _finalize_keywords_report(
        buckets=buckets,
        reporting_source=reporting_source,
        keyword_source=keyword_source,
        filters=filters,
        sort_by=sort_by,
        order=order,
    )


def _build_keywords_report_sql(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    keywords: list[KeywordDefinition],
    filters: ReportFilters,
    spam_threshold: float,
    sort_by: str,
    order: str,
) -> dict[str, Any]:
    logger.info("Starting keywords report (SQL): source=%s keywords=%d", reporting_source.source_name, len(keywords))
    if not isinstance(reporting_source, SqlReportingSource):
        raise TypeError(
            "SQL keywords report requires a source implementing "
            "SqlReportingSource (build_keywords_report_data)"
        )
    raw_data = reporting_source.build_keywords_report_data(
        keywords=keywords,
        filters=filters,
        spam_threshold=spam_threshold,
    )

    buckets: dict[str, dict[str, Any]] = {}
    for keyword in keywords:
        buckets[keyword.keyword_id] = {
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

    for raw_row in raw_data:
        row = cast(dict[str, Any], raw_row)
        kid = row["keyword_id"]
        bucket = buckets.get(kid)
        if bucket is None:
            continue
        bucket["matched_calls"] = int(row.get("matched_calls", 0))
        bucket["total_matches"] = int(row.get("total_matches", 0))
        bucket["matched_managers"] = {m for m in row.get("top_managers", [])}
        bucket["intents"] = {kv[0]: kv[1] for kv in row.get("top_intents", [])}
        bucket["outcomes"] = {kv[0]: kv[1] for kv in row.get("top_outcomes", [])}

    return _finalize_keywords_report(
        buckets=buckets,
        reporting_source=reporting_source,
        keyword_source=keyword_source,
        filters=filters,
        sort_by=sort_by,
        order=order,
    )


def _finalize_keywords_report(
    buckets: dict[str, dict[str, Any]],
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    filters: ReportFilters,
    sort_by: str,
    order: str,
) -> dict[str, Any]:
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
                "top_intents": sorted(
                    bucket["intents"].items(), key=lambda kv: kv[1], reverse=True
                )[:10],
                "top_outcomes": sorted(
                    bucket["outcomes"].items(), key=lambda kv: kv[1], reverse=True
                )[:5],
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
            key=lambda item: (
                item.get(sort_by, 0),
                item["category"],
                item["label"],
                item["keyword_id"],
            ),
            reverse=reverse,
        )

    return {
        "generated_at": _utc_now_iso(),
        "report_data_source": reporting_source.source_name,
        "keyword_data_source": keyword_source.source_name,
        "filters": filters.as_dict(),
        "total_keywords": len(result_keywords),
        "keywords_with_matches": sum(
            1 for item in result_keywords if item["matched_calls"] > 0
        ),
        "keywords": result_keywords,
    }
