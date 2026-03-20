import os
import re
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from api.schemas import (
    CustomersSortQuery,
    KeywordCallsSortQuery,
    KeywordManagersSortQuery,
    KeywordsSortQuery,
    ManagersSortQuery,
    PaginationQuery,
    ReportFiltersQuery,
)
from core.keywords_service import build_keywords_report
from core.reporting_service import (
    build_customer_followup_report,
    build_customers_report,
    build_managers_report,
    build_overall_report,
)
from domain.config import ANALYSIS, KEYWORDS_CONFIG
from domain.reporting import ReportFilters

router = APIRouter(prefix="/reports", tags=["reports"])

_SAFE_ID = re.compile(r"^[\w\-]+$")
_SAFE_ID_PATTERN = r"^[\w\-]+$"

_SOURCE_AND_THRESHOLD_NOTE = (
    "Data source is selected automatically: Postgres when `POSTGRES_DSN` is configured, "
    "otherwise JSON/YAML files. Spam metrics use `SPAM_PROBABILITY_THRESHOLD` (default `0.7`)."
)


def _build_filters(query: ReportFiltersQuery) -> ReportFilters:
    return ReportFilters(
        date_from=query.date_from,
        date_to=query.date_to,
        manager_id=query.manager_id,
        role=query.role,
        direction=query.direction,
        intent=query.intent,
        outcome=query.outcome,
        spam_only=query.spam_only,
        effective_only=query.effective_only,
    )


def _get_reporting_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresReportingSource(dsn)
    return JsonReportingSource(ANALYSIS)


def _get_keyword_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresKeywordSource(dsn)
    return YamlKeywordSource(KEYWORDS_CONFIG)


def _get_materialized_keyword_source() -> PostgresKeywordSource:
    source = _get_keyword_source()
    if not isinstance(source, PostgresKeywordSource):
        source.close()
        raise HTTPException(status_code=405, detail="Keyword drill-down requires POSTGRES_DSN")
    if not source.is_materialized():
        source.close()
        raise HTTPException(status_code=409, detail="Keyword matches are not materialized yet")
    return source


@router.get(
    "/overall",
    summary="Overall KPI report",
    description=(
        "Returns aggregate KPI counters for all calls that match filters.\n\n"
        "**Defaults**\n"
        "- When filters are omitted, the whole dataset is used.\n"
        "- `spam_only=false`, `effective_only=false`.\n"
        f"- {_SOURCE_AND_THRESHOLD_NOTE}\n\n"
        "**Example**\n"
        "`GET /reports/overall?date_from=2026-03-01&date_to=2026-03-31&spam_only=true`"
    ),
    responses={
        200: {"description": "Overall report with totals, top intents/outcomes/questions, and active filter echo."},
    },
)
def overall_report(query: Annotated[ReportFiltersQuery, Depends()]):
    filters = _build_filters(query)
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return build_overall_report(source, filters, spam_threshold)
    finally:
        source.close()


@router.get(
    "/managers",
    summary="Managers performance report",
    description=(
        "Returns per-manager stats plus grouped views by role.\n\n"
        "**Defaults**\n"
        "- `sort_by=total_calls`\n"
        "- `order=desc`\n"
        f"- {_SOURCE_AND_THRESHOLD_NOTE}\n\n"
        "**Example**\n"
        "`GET /reports/managers?role=sales&sort_by=effective_calls&order=desc`"
    ),
    responses={
        200: {"description": "Manager aggregates under `all_managers` and `by_role`."},
    },
)
def managers_report(
    query: Annotated[ReportFiltersQuery, Depends()],
    sorting: Annotated[ManagersSortQuery, Depends()],
):
    filters = _build_filters(query)
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return build_managers_report(source, filters, spam_threshold, sorting.sort_by.value, sorting.order.value)
    finally:
        source.close()


@router.get(
    "/customers",
    summary="Customers report",
    description=(
        "Builds customer-centric follow-up aggregates by inferred customer phone.\n\n"
        "**Defaults**\n"
        "- `sort_by=total_calls`\n"
        "- `order=desc`\n"
        f"- {_SOURCE_AND_THRESHOLD_NOTE}\n\n"
        "**Example**\n"
        "`GET /reports/customers?manager_id=petrenko_aa&sort_by=last_call_date&order=desc`"
    ),
    responses={
        200: {"description": "Customer aggregates under `all_customers` with first/last call dates and top metrics."},
    },
)
def customers_report(
    query: Annotated[ReportFiltersQuery, Depends()],
    sorting: Annotated[CustomersSortQuery, Depends()],
):
    filters = _build_filters(query)
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return build_customers_report(source, filters, spam_threshold, sorting.sort_by.value, sorting.order.value)
    finally:
        source.close()


@router.get(
    "/customers/{customer_phone}",
    summary="Customer follow-up detail",
    description=(
        "Returns full follow-up history for a specific customer phone.\n\n"
        "The path value is normalized by removing non-digits before lookup.\n\n"
        "**Example**\n"
        "`GET /reports/customers/%2B38%28067%29123-45-67?effective_only=true`"
    ),
    responses={
        200: {"description": "Detailed customer report with aggregated stats and the matching call list."},
        400: {
            "description": "Invalid customer phone.",
            "content": {"application/json": {"example": {"detail": "Invalid customer_phone"}}},
        },
        404: {
            "description": "No customer found for the normalized phone within selected filters.",
            "content": {"application/json": {"example": {"detail": "Customer report not found"}}},
        },
    },
)
def customer_report(
    customer_phone: Annotated[
        str,
        Path(
            description="Customer phone in any readable format. Non-digit characters are ignored.",
            example="+38 (067) 123-45-67",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
):
    normalized_phone = re.sub(r"[^\d]", "", customer_phone)
    if not normalized_phone:
        raise HTTPException(status_code=400, detail="Invalid customer_phone")

    filters = _build_filters(query)
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        data = build_customer_followup_report(source, filters, spam_threshold, normalized_phone)
    finally:
        source.close()

    if data is None:
        raise HTTPException(status_code=404, detail="Customer report not found")
    return data


@router.get(
    "/manager/{manager_id}",
    summary="Single manager report",
    description=(
        "Returns one manager aggregate record.\n\n"
        "The path `manager_id` always takes precedence over any `manager_id` query value.\n\n"
        "**Example**\n"
        "`GET /reports/manager/petrenko_aa?date_from=2026-03-01&date_to=2026-03-31`"
    ),
    responses={
        200: {"description": "Single manager aggregate payload."},
        400: {
            "description": "Invalid manager id format.",
            "content": {"application/json": {"example": {"detail": "Invalid manager_id"}}},
        },
        404: {
            "description": "Manager is not present in filtered results.",
            "content": {"application/json": {"example": {"detail": "Manager report not found"}}},
        },
    },
)
def manager_report(
    manager_id: Annotated[
        str,
        Path(
            description="Manager identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            example="petrenko_aa",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
):
    if not _SAFE_ID.match(manager_id):
        raise HTTPException(status_code=400, detail="Invalid manager_id")

    filters = _build_filters(query)
    filters = ReportFilters(
        date_from=filters.date_from,
        date_to=filters.date_to,
        manager_id=manager_id,
        role=filters.role,
        direction=filters.direction,
        intent=filters.intent,
        outcome=filters.outcome,
        spam_only=filters.spam_only,
        effective_only=filters.effective_only,
    )
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        data = build_managers_report(source, filters, spam_threshold)
    finally:
        source.close()
    for manager in data.get("all_managers", []):
        if manager.get("manager_id") == manager_id:
            return manager

    raise HTTPException(status_code=404, detail="Manager report not found")


@router.get(
    "/keywords",
    summary="Keywords aggregate report",
    description=(
        "Returns keyword-level match statistics.\n\n"
        "When keyword matches are materialized in Postgres, this endpoint uses materialized data; "
        "otherwise it computes metrics dynamically.\n\n"
        "**Defaults**\n"
        "- `sort_by=matched_calls`\n"
        "- `order=desc`\n"
        f"- {_SOURCE_AND_THRESHOLD_NOTE}\n\n"
        "**Example**\n"
        "`GET /reports/keywords?intent=order_status&sort_by=total_matches&order=desc`"
    ),
    responses={
        200: {"description": "Keyword aggregates with match counters and coverage metadata."},
    },
)
def keywords_report(
    query: Annotated[ReportFiltersQuery, Depends()],
    sorting: Annotated[KeywordsSortQuery, Depends()],
):
    filters = _build_filters(query)
    reporting_source = _get_reporting_source()
    keyword_source = _get_keyword_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        if (
            isinstance(reporting_source, PostgresReportingSource)
            and isinstance(keyword_source, PostgresKeywordSource)
            and keyword_source.is_materialized()
        ):
            return keyword_source.build_materialized_keywords_report(
                filters,
                spam_threshold,
                sorting.sort_by.value,
                sorting.order.value,
            )
        return build_keywords_report(
            reporting_source,
            keyword_source,
            filters,
            spam_threshold,
            sorting.sort_by.value,
            sorting.order.value,
        )
    finally:
        reporting_source.close()
        keyword_source.close()


@router.get(
    "/keywords/{keyword_id}",
    summary="Single keyword aggregate",
    description=(
        "Returns aggregate statistics for one keyword id.\n\n"
        "Works with both materialized and dynamic keyword reporting modes.\n\n"
        "**Example**\n"
        "`GET /reports/keywords/delivery?date_from=2026-03-01`"
    ),
    responses={
        200: {"description": "Single keyword aggregate payload."},
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword is not present in filtered results.",
            "content": {"application/json": {"example": {"detail": "Keyword report not found"}}},
        },
    },
)
def keyword_detail_report(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            example="delivery",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")

    filters = _build_filters(query)
    reporting_source = _get_reporting_source()
    keyword_source = _get_keyword_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        if (
            isinstance(reporting_source, PostgresReportingSource)
            and isinstance(keyword_source, PostgresKeywordSource)
            and keyword_source.is_materialized()
        ):
            data = keyword_source.build_materialized_keywords_report(filters, spam_threshold)
        else:
            data = build_keywords_report(reporting_source, keyword_source, filters, spam_threshold)
    finally:
        reporting_source.close()
        keyword_source.close()

    for keyword in data.get("keywords", []):
        if keyword.get("keyword_id") == keyword_id:
            return keyword

    raise HTTPException(status_code=404, detail="Keyword report not found")


@router.get(
    "/keywords/{keyword_id}/calls",
    summary="Keyword matched calls (paginated)",
    description=(
        "Returns paginated call list where the selected keyword was matched.\n\n"
        "**Requirements**\n"
        "- `POSTGRES_DSN` must be configured.\n"
        "- Keyword matches must be materialized (`POST /keywords/materialize`).\n\n"
        "**Defaults**\n"
        "- `limit=50`, `offset=0`\n"
        "- `sort_by=call_date`, `order=desc`\n\n"
        "**Example**\n"
        "`GET /reports/keywords/delivery/calls?limit=25&offset=0&sort_by=match_count&order=desc`"
    ),
    responses={
        200: {"description": "Paginated matched calls and pagination metadata."},
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword is not found in catalog.",
            "content": {"application/json": {"example": {"detail": "Keyword report not found"}}},
        },
        405: {
            "description": "Drill-down is unavailable without Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword drill-down requires POSTGRES_DSN"}}
            },
        },
        409: {
            "description": "Materialized keyword matches are not prepared yet.",
            "content": {
                "application/json": {"example": {"detail": "Keyword matches are not materialized yet"}}
            },
        },
    },
)
def keyword_calls_report(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            example="delivery",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
    pagination: Annotated[PaginationQuery, Depends()],
    sorting: Annotated[KeywordCallsSortQuery, Depends()],
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    filters = _build_filters(query)
    source = _get_materialized_keyword_source()
    if source.get_keyword(keyword_id) is None:
        source.close()
        raise HTTPException(status_code=404, detail="Keyword report not found")
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return source.build_keyword_calls_report(
            keyword_id=keyword_id,
            filters=filters,
            spam_threshold=spam_threshold,
            limit=pagination.limit,
            offset=pagination.offset,
            sort_by=sorting.sort_by.value,
            order=sorting.order.value,
        )
    finally:
        source.close()


@router.get(
    "/keywords/{keyword_id}/trend",
    summary="Keyword trend over time",
    description=(
        "Returns trend points for the selected keyword over dates in the filtered range.\n\n"
        "**Requirements**\n"
        "- `POSTGRES_DSN` must be configured.\n"
        "- Keyword matches must be materialized (`POST /keywords/materialize`).\n\n"
        "**Example**\n"
        "`GET /reports/keywords/delivery/trend?date_from=2026-03-01&date_to=2026-03-20`"
    ),
    responses={
        200: {"description": "Date-based trend series for one keyword."},
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword is not found in catalog.",
            "content": {"application/json": {"example": {"detail": "Keyword report not found"}}},
        },
        405: {
            "description": "Drill-down is unavailable without Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword drill-down requires POSTGRES_DSN"}}
            },
        },
        409: {
            "description": "Materialized keyword matches are not prepared yet.",
            "content": {
                "application/json": {"example": {"detail": "Keyword matches are not materialized yet"}}
            },
        },
    },
)
def keyword_trend_report(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            example="delivery",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    filters = _build_filters(query)
    source = _get_materialized_keyword_source()
    if source.get_keyword(keyword_id) is None:
        source.close()
        raise HTTPException(status_code=404, detail="Keyword report not found")
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return source.build_keyword_trend_report(keyword_id=keyword_id, filters=filters, spam_threshold=spam_threshold)
    finally:
        source.close()


@router.get(
    "/keywords/{keyword_id}/managers",
    summary="Keyword manager distribution",
    description=(
        "Returns manager-level breakdown for one keyword.\n\n"
        "**Requirements**\n"
        "- `POSTGRES_DSN` must be configured.\n"
        "- Keyword matches must be materialized (`POST /keywords/materialize`).\n\n"
        "**Defaults**\n"
        "- `sort_by=matched_calls`\n"
        "- `order=desc`\n\n"
        "**Example**\n"
        "`GET /reports/keywords/delivery/managers?sort_by=total_matches&order=desc`"
    ),
    responses={
        200: {"description": "Manager-level aggregates for one keyword."},
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword is not found in catalog.",
            "content": {"application/json": {"example": {"detail": "Keyword report not found"}}},
        },
        405: {
            "description": "Drill-down is unavailable without Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword drill-down requires POSTGRES_DSN"}}
            },
        },
        409: {
            "description": "Materialized keyword matches are not prepared yet.",
            "content": {
                "application/json": {"example": {"detail": "Keyword matches are not materialized yet"}}
            },
        },
    },
)
def keyword_managers_report(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            example="delivery",
        ),
    ],
    query: Annotated[ReportFiltersQuery, Depends()],
    sorting: Annotated[KeywordManagersSortQuery, Depends()],
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    filters = _build_filters(query)
    source = _get_materialized_keyword_source()
    if source.get_keyword(keyword_id) is None:
        source.close()
        raise HTTPException(status_code=404, detail="Keyword report not found")
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return source.build_keyword_managers_report(
            keyword_id=keyword_id,
            filters=filters,
            spam_threshold=spam_threshold,
            sort_by=sorting.sort_by.value,
            order=sorting.order.value,
        )
    finally:
        source.close()
