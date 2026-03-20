from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from domain.reporting import ReportCallRecord, ReportFilters


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _record_from_analysis(call_id: str, data: dict[str, Any]) -> ReportCallRecord:
    call_meta = data.get("call_meta") or {}

    spam_probability = data.get("spam_probability", 0.0)
    try:
        spam_probability = float(spam_probability)
    except (TypeError, ValueError):
        spam_probability = 0.0

    effective_call = data.get("effective_call")
    if isinstance(effective_call, str):
        effective_call = effective_call.strip().lower() in {"1", "true", "yes", "tak", "так"}
    else:
        effective_call = bool(effective_call)

    audio_seconds = call_meta.get("audio_seconds", 0.0)
    try:
        audio_seconds = float(audio_seconds)
    except (TypeError, ValueError):
        audio_seconds = 0.0

    return ReportCallRecord(
        call_id=call_id,
        manager_id=str(data.get("manager_id") or "manager_unknown"),
        manager_name=str(data.get("manager_name") or "Unknown/General"),
        role=str(data.get("role") or "unknown"),
        direction=str(call_meta.get("direction") or data.get("direction") or "unknown"),
        spam_probability=spam_probability,
        effective_call=effective_call,
        intent=str(data.get("intent") or "інше"),
        outcome=str(data.get("outcome") or "невідомо"),
        summary=str(data.get("summary") or ""),
        audio_seconds=audio_seconds,
        call_date=str(call_meta.get("date") or ""),
        src_number=str(call_meta.get("src_number") or ""),
        dst_number=str(call_meta.get("dst_number") or ""),
        key_questions=_as_str_list(data.get("key_questions")),
        objections=_as_str_list(data.get("objections")),
    )


class JsonReportingSource:
    source_name = "json"

    def __init__(self, analysis_dir: Path):
        self.analysis_dir = analysis_dir

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]:
        if not self.analysis_dir.exists():
            return

        for path in sorted(self.analysis_dir.glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            record = _record_from_analysis(path.stem, data)
            if filters.matches_record(record):
                yield record

    def close(self) -> None:
        return None
