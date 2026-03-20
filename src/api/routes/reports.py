import os
import re
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

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


@router.get("/overall")
def overall_report(query: Annotated[ReportFiltersQuery, Depends()]):
    filters = _build_filters(query)
    source = _get_reporting_source()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    try:
        return build_overall_report(source, filters, spam_threshold)
    finally:
        source.close()


@router.get("/managers")
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


@router.get("/customers")
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


@router.get("/customers/{customer_phone}")
def customer_report(customer_phone: str, query: Annotated[ReportFiltersQuery, Depends()]):
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


@router.get("/manager/{manager_id}")
def manager_report(manager_id: str, query: Annotated[ReportFiltersQuery, Depends()]):
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


@router.get("/keywords")
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


@router.get("/keywords/{keyword_id}")
def keyword_detail_report(keyword_id: str, query: Annotated[ReportFiltersQuery, Depends()]):
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


@router.get("/keywords/{keyword_id}/calls")
def keyword_calls_report(
    keyword_id: str,
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


@router.get("/keywords/{keyword_id}/trend")
def keyword_trend_report(keyword_id: str, query: Annotated[ReportFiltersQuery, Depends()]):
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


@router.get("/keywords/{keyword_id}/managers")
def keyword_managers_report(
    keyword_id: str,
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
