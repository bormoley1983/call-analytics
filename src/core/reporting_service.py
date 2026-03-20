from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from domain.reporting import ReportCallRecord, ReportFilters
from ports.reporting import ReportingSource


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _include_record(record: ReportCallRecord, filters: ReportFilters, spam_threshold: float) -> bool:
    if not filters.matches_record(record):
        return False
    if filters.spam_only and record.spam_probability < spam_threshold:
        return False
    if filters.effective_only and not record.effective_call:
        return False
    return True


def build_overall_report(
    source: ReportingSource,
    filters: ReportFilters,
    spam_threshold: float,
) -> dict[str, Any]:
    total_calls = 0
    spam_calls = 0
    effective_calls = 0
    total_duration = 0.0
    intents: dict[str, int] = {}
    outcomes: dict[str, int] = {}
    questions: dict[str, int] = {}
    managers: set[str] = set()

    for record in source.iter_call_records(filters):
        if not _include_record(record, filters, spam_threshold):
            continue

        total_calls += 1
        total_duration += record.audio_seconds
        managers.add(record.manager_id)

        if record.spam_probability >= spam_threshold:
            spam_calls += 1
        if record.effective_call:
            effective_calls += 1

        intents[record.intent] = intents.get(record.intent, 0) + 1
        outcomes[record.outcome] = outcomes.get(record.outcome, 0) + 1

        for question in record.key_questions:
            normalized = question.lower().strip()
            if normalized:
                questions[normalized] = questions.get(normalized, 0) + 1

    return {
        "generated_at": _utc_now_iso(),
        "data_source": source.source_name,
        "filters": filters.as_dict(),
        "total_calls": total_calls,
        "analyzed_calls": total_calls,
        "unique_managers": len(managers),
        "skipped_metrics_available": False,
        "spam_calls": spam_calls,
        "effective_calls": effective_calls,
        "total_duration_seconds": total_duration,
        "top_intents": sorted(intents.items(), key=lambda kv: kv[1], reverse=True)[:10],
        "top_outcomes": sorted(outcomes.items(), key=lambda kv: kv[1], reverse=True)[:5],
        "top_questions": sorted(questions.items(), key=lambda kv: kv[1], reverse=True)[:10],
    }


def _manager_bucket(record: ReportCallRecord) -> dict[str, Any]:
    return {
        "manager_id": record.manager_id,
        "manager_name": record.manager_name,
        "role": record.role,
        "total_calls": 0,
        "incoming": 0,
        "outgoing": 0,
        "spam_calls": 0,
        "effective_calls": 0,
        "total_duration_seconds": 0.0,
        "intents": {},
        "outcomes": {},
        "questions": {},
    }


def _finalize_manager_stats(stats: dict[str, Any]) -> dict[str, Any]:
    result = dict(stats)
    result["top_intents"] = sorted(result.pop("intents").items(), key=lambda kv: kv[1], reverse=True)[:10]
    result["top_outcomes"] = sorted(result.pop("outcomes").items(), key=lambda kv: kv[1], reverse=True)[:5]
    result["top_questions"] = sorted(result.pop("questions").items(), key=lambda kv: kv[1], reverse=True)[:10]
    return result


def build_managers_report(
    source: ReportingSource,
    filters: ReportFilters,
    spam_threshold: float,
    sort_by: str = "total_calls",
    order: str = "desc",
) -> dict[str, Any]:
    role_summary: dict[str, dict[str, int]] = {}
    managers_stats: dict[str, dict[str, Any]] = {}

    for record in source.iter_call_records(filters):
        if not _include_record(record, filters, spam_threshold):
            continue

        stats = managers_stats.setdefault(record.manager_id, _manager_bucket(record))
        stats["total_calls"] += 1
        stats["total_duration_seconds"] += record.audio_seconds

        if record.direction == "incoming":
            stats["incoming"] += 1
        elif record.direction == "outgoing":
            stats["outgoing"] += 1

        if record.spam_probability >= spam_threshold:
            stats["spam_calls"] += 1
        if record.effective_call:
            stats["effective_calls"] += 1

        stats["intents"][record.intent] = stats["intents"].get(record.intent, 0) + 1
        stats["outcomes"][record.outcome] = stats["outcomes"].get(record.outcome, 0) + 1

        for question in record.key_questions:
            normalized = question.lower().strip()
            if normalized:
                stats["questions"][normalized] = stats["questions"].get(normalized, 0) + 1

        role = record.role or "unknown"
        summary = role_summary.setdefault(role, {"total_calls": 0})
        summary["total_calls"] += 1

    all_managers = [
        _finalize_manager_stats(stats)
        for _, stats in sorted(
            managers_stats.items(),
            key=lambda item: (item[1]["role"], item[1]["manager_name"], item[0]),
        )
    ]

    reverse = order == "desc"
    if sort_by == "manager_name":
        all_managers.sort(key=lambda item: (item["manager_name"], item["manager_id"]), reverse=reverse)
    else:
        all_managers.sort(
            key=lambda item: (item.get(sort_by, 0), item["manager_name"], item["manager_id"]),
            reverse=reverse,
        )

    by_role: dict[str, list[dict[str, Any]]] = {}
    for manager in all_managers:
        by_role.setdefault(manager["role"], []).append(manager)

    return {
        "generated_at": _utc_now_iso(),
        "data_source": source.source_name,
        "filters": filters.as_dict(),
        "skipped_metrics_available": False,
        "role_summary": role_summary,
        "by_role": by_role,
        "all_managers": all_managers,
        "total_managers": len(all_managers),
    }
