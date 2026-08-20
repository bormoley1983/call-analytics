from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, runtime_checkable

from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters


class ReportingSource(Protocol):
    source_name: str

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]: ...

    def close(self) -> None: ...


@runtime_checkable
class SqlReportingSource(ReportingSource, Protocol):
    """Structural protocol for sources that build report data in SQL.

    Only the Postgres adapter implements these methods; JSON/YAML fallback
    adapters do not. Core code narrows with ``isinstance(source,
    SqlReportingSource)`` (structural check) instead of ``hasattr`` +
    ``type: ignore``.
    """

    def build_overall_report_data(
        self, filters: ReportFilters, spam_threshold: float
    ) -> dict[str, object]: ...

    def build_managers_report_data(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "total_calls",
        order: str = "desc",
    ) -> dict[str, object]: ...

    def build_customers_report_data(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "total_calls",
        order: str = "desc",
    ) -> dict[str, object]: ...

    def build_keywords_report_data(
        self,
        *,
        keywords: list[KeywordDefinition],
        filters: ReportFilters,
        spam_threshold: float,
    ) -> list[dict[str, object]]: ...
