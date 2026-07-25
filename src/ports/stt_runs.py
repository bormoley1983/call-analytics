from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from domain.stt_runs import SttRunManifest, SttRunResultRecord


class SttRunStorePort(Protocol):
    def ensure_ready(self) -> None: ...

    def create_run(self, run: SttRunManifest) -> None: ...

    def update_run_status(self, run_id: str, status: str) -> None: ...

    def update_run_counters(
        self,
        run_id: str,
        *,
        total_calls: int,
        completed_calls: int,
        failed_calls: int,
        skipped_calls: int,
    ) -> None: ...

    def upsert_result(self, result: SttRunResultRecord) -> None: ...

    def load_run(self, run_id: str) -> SttRunManifest: ...

    def load_result(self, run_id: str, call_id: str) -> SttRunResultRecord: ...

    def iter_results(self, run_id: str) -> Iterator[SttRunResultRecord]: ...

    def close(self) -> None: ...
