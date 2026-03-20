from __future__ import annotations

from typing import Iterable, Protocol

from domain.reporting import ReportCallRecord, ReportFilters


class ReportingSource(Protocol):
    source_name: str

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]: ...

    def close(self) -> None: ...
