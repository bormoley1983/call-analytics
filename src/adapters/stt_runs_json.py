from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from domain.stt_runs import SttRunManifest, SttRunResultRecord
from ports.stt_runs import SttRunStorePort


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported type for JSON serialization: {type(value)!r}")


class JsonSttRunStore(SttRunStorePort):
    def __init__(self, out_root: Path):
        self._root = out_root / "stt_runs"

    def ensure_ready(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, run_id: str) -> Path:
        return self._root / run_id

    def _manifest_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "manifest.json"

    def _result_path(self, run_id: str, call_id: str) -> Path:
        return self._run_dir(run_id) / "results" / f"{call_id}.json"

    def create_run(self, run: SttRunManifest) -> None:
        run_dir = self._run_dir(run.run_id)
        (run_dir / "results").mkdir(parents=True, exist_ok=True)
        self._manifest_path(run.run_id).write_text(
            json.dumps(asdict(run), ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )

    def update_run_status(self, run_id: str, status: str) -> None:
        run = self.load_run(run_id)
        payload = asdict(run)
        payload["status"] = status
        self._manifest_path(run_id).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def update_run_counters(
        self,
        run_id: str,
        *,
        total_calls: int,
        completed_calls: int,
        failed_calls: int,
        skipped_calls: int,
    ) -> None:
        run = self.load_run(run_id)
        payload = asdict(run)
        payload.update(
            {
                "total_calls": total_calls,
                "completed_calls": completed_calls,
                "failed_calls": failed_calls,
                "skipped_calls": skipped_calls,
            }
        )
        self._manifest_path(run_id).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def upsert_result(self, result: SttRunResultRecord) -> None:
        out = self._result_path(result.run_id, result.call_id)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(asdict(result), ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )

    def load_run(self, run_id: str) -> SttRunManifest:
        payload = json.loads(self._manifest_path(run_id).read_text(encoding="utf-8"))
        return SttRunManifest(**payload)

    def load_result(self, run_id: str, call_id: str) -> SttRunResultRecord:
        payload = json.loads(self._result_path(run_id, call_id).read_text(encoding="utf-8"))
        return SttRunResultRecord(**payload)

    def iter_results(self, run_id: str) -> Iterator[SttRunResultRecord]:
        results_dir = self._run_dir(run_id) / "results"
        if not results_dir.exists():
            return
        for path in sorted(results_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            yield SttRunResultRecord(**payload)

    def close(self) -> None:
        return None
