from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class JsonStorage:
    def __init__(self, out: Path, norm: Path, trans: Path, analysis: Path):
        self.out = out
        self.norm = norm
        self.trans = trans
        self.analysis = analysis
        self.calls = out / "calls"

    # --- lifecycle ---

    def ensure_ready(self) -> None:
        for p in [self.out, self.norm, self.trans, self.analysis, self.calls]:
            p.mkdir(parents=True, exist_ok=True)

    def ensure_dirs(self) -> None:
        self.ensure_ready()

    def close(self) -> None:
        pass

    # --- internal path helpers (also used by planner + migrate_storage) ---

    def transcript_path(self, call_id: str) -> Path:
        return self.trans / f"{call_id}.json"

    def analysis_path(self, call_id: str) -> Path:
        return self.analysis / f"{call_id}.json"

    def call_metadata_path(self, call_id: str) -> Path:
        return self.calls / f"{call_id}.json"

    def load_call_metadata(self, call_id: str) -> Dict[str, Any]:
        path = self.call_metadata_path(call_id)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def upsert_call_metadata(
        self,
        *,
        call_id: str,
        source_file: str | None = None,
        source_path: str | None = None,
        call_datetime: datetime | None = None,
        status: str = "discovered",
        error_message: str | None = None,
        mark_synced: bool = False,
    ) -> None:
        current = self.load_call_metadata(call_id)

        if not current:
            current = {
                "call_id": call_id,
                "source_file": source_file,
                "source_path": source_path,
                "call_datetime": call_datetime.isoformat() if call_datetime else None,
                "status": status,
                "error_message": error_message,
                "discovered_at": None,
                "transcribed_at": None,
                "translated_at": None,
                "analyzed_at": None,
                "synced_at": None,
            }

        current["call_id"] = call_id
        if source_file:
            current["source_file"] = source_file
        if source_path:
            current["source_path"] = source_path
        if call_datetime:
            current["call_datetime"] = call_datetime.isoformat()
        current["status"] = status
        if error_message:
            current["error_message"] = error_message

        if status == "discovered" and not current.get("discovered_at"):
            current["discovered_at"] = _utc_now_iso()
        if status in {"transcribed", "translated", "processed"} and not current.get(
            "transcribed_at"
        ):
            current["transcribed_at"] = _utc_now_iso()
        if status in {"translated", "processed"} and not current.get("translated_at"):
            current["translated_at"] = _utc_now_iso()
        if status == "processed" and not current.get("analyzed_at"):
            current["analyzed_at"] = _utc_now_iso()
        if mark_synced and not current.get("synced_at"):
            current["synced_at"] = _utc_now_iso()

        self.call_metadata_path(call_id).write_text(
            json.dumps(current, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # --- StoragePort interface ---

    def transcript_exists(self, call_id: str) -> bool:
        return self.transcript_path(call_id).exists()

    def analysis_exists(self, call_id: str) -> bool:
        return self.analysis_path(call_id).exists()

    def load_transcript(self, call_id: str) -> Dict[str, Any]:
        return json.loads(self.transcript_path(call_id).read_text(encoding="utf-8"))

    def load_analysis(self, call_id: str) -> Dict[str, Any]:
        return json.loads(self.analysis_path(call_id).read_text(encoding="utf-8"))

    def save_transcript(self, call_id: str, data: Dict[str, Any]) -> None:
        self.transcript_path(call_id).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        raw_call_meta = data.get("call_meta")
        call_meta: dict[str, Any] = (
            raw_call_meta if isinstance(raw_call_meta, dict) else {}
        )
        stage = str(data.get("_pipeline_stage") or "transcribed")
        status = "translated" if stage == "translated" else "transcribed"
        date_str = str(call_meta.get("date") or "").strip()
        call_dt = (
            datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            if date_str
            else None
        )
        self.upsert_call_metadata(
            call_id=call_id,
            call_datetime=call_dt,
            status=status,
        )

    def save_analysis(self, call_id: str, data: Dict[str, Any]) -> None:
        self.analysis_path(call_id).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        raw_call_meta = data.get("call_meta")
        call_meta: dict[str, Any] = (
            raw_call_meta if isinstance(raw_call_meta, dict) else {}
        )
        date_str = str(call_meta.get("date") or "").strip()
        call_dt = (
            datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            if date_str
            else None
        )
        self.upsert_call_metadata(
            call_id=call_id,
            source_file=str(data.get("source_file") or "") or None,
            source_path=str(data.get("source_path") or "") or None,
            call_datetime=call_dt,
            status="processed",
            error_message=str(data.get("analysis_error") or "") or None,
        )

    def mark_analysis_stale_if_text_changed(
        self, call_id: str, text_sha256: str
    ) -> bool:
        path = self.analysis_path(call_id)
        if not path.exists():
            return False
        data = json.loads(path.read_text(encoding="utf-8"))
        old_hash = str(data.get("input_text_sha256") or "")
        if old_hash == text_sha256:
            return False
        path.unlink(missing_ok=True)
        return True

    def promote_stt_result(
        self,
        call_id: str,
        transcript: Dict[str, Any],
        *,
        stt_run_id: str,
        stt_config_hash: str,
        source_text_sha256: str,
    ) -> None:
        promoted = dict(transcript)
        promoted["stt_run_id"] = stt_run_id
        promoted["stt_config_hash"] = stt_config_hash
        promoted["source_text_sha256"] = source_text_sha256
        self.save_transcript(call_id, promoted)

    def sync_per_call(self, per_call: list) -> None:
        """Bulk-sync a pipeline's per_call results into JSON storage."""
        for item in per_call:
            meta = item.get("meta", {}) or {}
            call_id = meta.get("call_id")
            date_str = str(meta.get("date") or "").strip()
            call_dt = (
                datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
                if date_str
                else None
            )
            if call_id:
                self.upsert_call_metadata(
                    call_id=call_id,
                    source_file=meta.get("source_file"),
                    source_path=meta.get("source_path"),
                    call_datetime=call_dt,
                    status=item.get("status") or "discovered",
                    error_message=(
                        "duration_below_min_seconds"
                        if item.get("status") == "skipped_too_short"
                        else None
                    ),
                )

            if item.get("status") != "processed":
                continue
            if not call_id:
                continue
            self.save_analysis(call_id, item.get("analysis", {}))
