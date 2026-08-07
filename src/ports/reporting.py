from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from domain.reporting import ReportCallRecord, ReportFilters


class ReportingSource(Protocol):
    source_name: str

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]: ...

    def close(self) -> None: ...
