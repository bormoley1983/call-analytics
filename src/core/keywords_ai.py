from __future__ import annotations

from typing import Any

from core.reporting_service import build_customers_report
from core.keywords_service import build_keywords_report, list_keywords
from domain.reporting import ReportFilters


def prepare_keyword_catalog_analysis_input(
    *,
    keyword_source: Any,
    reporting_source: Any | None,
    include_inactive: bool = False,
    include_match_stats: bool = True,
    keyword_ids: list[str] | None = None,
    max_keywords: int = 100,
    spam_threshold: float = 0.7,
    max_customers: int = 20,
) -> dict[str, Any]:
    requested_ids = set(keyword_ids or [])
    catalog = list_keywords(keyword_source)
    stats_by_id: dict[str, dict[str, Any]] = {}
    customer_context: list[dict[str, Any]] = []
    reporting_source_name = None if reporting_source is None else reporting_source.source_name

    if include_match_stats and reporting_source is not None:
        report = build_keywords_report(
            reporting_source=reporting_source,
            keyword_source=keyword_source,
            filters=ReportFilters(),
            spam_threshold=spam_threshold,
            sort_by="matched_calls",
            order="desc",
        )
        stats_by_id = {item["keyword_id"]: item for item in report["keywords"]}
        customers_report = build_customers_report(
            source=reporting_source,
            filters=ReportFilters(),
            spam_threshold=spam_threshold,
            sort_by="total_calls",
            order="desc",
        )
        customer_context = [
            {
                "customer_phone": item["customer_phone"],
                "display_phone": item["display_phone"],
                "total_calls": item["total_calls"],
                "effective_calls": item["effective_calls"],
                "spam_calls": item["spam_calls"],
                "last_call_date": item["last_call_date"],
                "top_intents": item["top_intents"][:5],
                "top_outcomes": item["top_outcomes"][:3],
                "top_questions": item["top_questions"][:5],
                "managers": item["managers"][:3],
            }
            for item in customers_report["all_customers"][:max_customers]
        ]

    analysis_keywords: list[dict[str, Any]] = []
    for keyword in catalog["keywords"]:
        if requested_ids and keyword["keyword_id"] not in requested_ids:
            continue
        if not include_inactive and not keyword["is_active"]:
            continue

        stats = stats_by_id.get(keyword["keyword_id"], {})
        analysis_keywords.append(
            {
                "keyword_id": keyword["keyword_id"],
                "label": keyword["label"],
                "category": keyword["category"],
                "terms": keyword["terms"],
                "match_fields": keyword["match_fields"],
                "is_active": keyword["is_active"],
                "matched_calls": int(stats.get("matched_calls", 0)),
                "total_matches": int(stats.get("total_matches", 0)),
                "matched_managers": int(stats.get("matched_managers", 0)),
                "top_intents": list(stats.get("top_intents", [])),
                "top_outcomes": list(stats.get("top_outcomes", [])),
            }
        )

    if include_match_stats:
        analysis_keywords.sort(
            key=lambda item: (-item["matched_calls"], -item["total_matches"], item["category"], item["label"], item["keyword_id"])
        )
    else:
        analysis_keywords.sort(key=lambda item: (item["category"], item["label"], item["keyword_id"]))

    total_candidates = len(analysis_keywords)
    analysis_keywords = analysis_keywords[:max_keywords]

    return {
        "keyword_source": catalog["data_source"],
        "reporting_source": reporting_source_name,
        "analyzed_keywords": len(analysis_keywords),
        "total_candidates_before_limit": total_candidates,
        "truncated": total_candidates > len(analysis_keywords),
        "keywords": analysis_keywords,
        "customer_context": customer_context,
    }


def run_keyword_catalog_analysis(
    *,
    request_data: dict[str, Any],
    keyword_source: Any,
    reporting_source: Any | None,
    llm: Any,
    analysis_store: Any | None,
    include_inactive: bool = False,
    include_match_stats: bool = True,
    keyword_ids: list[str] | None = None,
    max_keywords: int = 100,
    max_groups: int = 20,
    spam_threshold: float = 0.7,
    ai_model: str | None = None,
) -> dict[str, Any]:
    analysis_input = prepare_keyword_catalog_analysis_input(
        keyword_source=keyword_source,
        reporting_source=reporting_source,
        include_inactive=include_inactive,
        include_match_stats=include_match_stats,
        keyword_ids=keyword_ids,
        max_keywords=max_keywords,
        spam_threshold=spam_threshold,
    )
    save_fn = None
    if analysis_store is not None:
        save_fn = getattr(analysis_store, "save_analysis", None)

    if analysis_input["analyzed_keywords"] == 0:
        response = {
            **analysis_input,
            "ai_analysis": {
                "summary": "No keywords matched the selected analysis scope.",
                "groups": [],
                "ungrouped_keyword_ids": [],
                "global_recommendations": [],
            },
            "analysis_history": None,
        }
        if save_fn is not None:
            history = save_fn(
                request_data=request_data,
                analysis_input=analysis_input,
                ai_analysis=response["ai_analysis"],
                keyword_source=analysis_input["keyword_source"],
                reporting_source=analysis_input["reporting_source"],
                ai_model=ai_model,
            )
            response["analysis_history"] = history
        return response

    ai_analysis = llm.analyze_keyword_catalog(analysis_input, max_groups=max_groups)
    response = {
        **analysis_input,
        "ai_analysis": ai_analysis,
        "analysis_history": None,
    }
    if save_fn is not None:
        history = save_fn(
            request_data=request_data,
            analysis_input=analysis_input,
            ai_analysis=ai_analysis,
            keyword_source=analysis_input["keyword_source"],
            reporting_source=analysis_input["reporting_source"],
            ai_model=ai_model,
        )
        response["analysis_history"] = history
    return response
