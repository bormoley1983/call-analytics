from __future__ import annotations

import re
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


def _normalize_phone_number(value: str | None) -> str:
    digits = re.sub(r"[^\d]", "", str(value or ""))
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    # Treat local UA 10-digit mobile format as international to avoid split buckets.
    if len(digits) == 10 and digits.startswith("0"):
        digits = f"38{digits}"
    return digits


def _resolve_customer_phone(record: ReportCallRecord) -> tuple[str, str]:
    src_raw = (record.src_number or "").strip()
    dst_raw = (record.dst_number or "").strip()
    src_norm = _normalize_phone_number(src_raw)
    dst_norm = _normalize_phone_number(dst_raw)

    ordered_candidates: list[tuple[str, str]]
    if record.direction == "incoming":
        ordered_candidates = [(src_norm, src_raw), (dst_norm, dst_raw)]
    elif record.direction == "outgoing":
        ordered_candidates = [(dst_norm, dst_raw), (src_norm, src_raw)]
    else:
        ordered_candidates = sorted(
            [(src_norm, src_raw), (dst_norm, dst_raw)],
            key=lambda item: len(item[0]),
            reverse=True,
        )

    for normalized, raw in ordered_candidates:
        if normalized:
            return normalized, (raw or normalized)

    return "unknown", "unknown"


def _customer_bucket(customer_phone: str, display_phone: str) -> dict[str, Any]:
    return {
        "customer_phone": customer_phone,
        "display_phone": display_phone,
        "total_calls": 0,
        "incoming": 0,
        "outgoing": 0,
        "spam_calls": 0,
        "effective_calls": 0,
        "total_duration_seconds": 0.0,
        "first_call_date": None,
        "last_call_date": None,
        "intents": {},
        "outcomes": {},
        "questions": {},
        "managers": {},
    }


def _accumulate_customer_stats(
    stats: dict[str, Any],
    record: ReportCallRecord,
    spam_threshold: float,
) -> None:
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

    call_date = (record.call_date or "").strip()
    if call_date:
        first_call_date = stats.get("first_call_date")
        last_call_date = stats.get("last_call_date")
        if first_call_date is None or call_date < first_call_date:
            stats["first_call_date"] = call_date
        if last_call_date is None or call_date > last_call_date:
            stats["last_call_date"] = call_date

    stats["intents"][record.intent] = stats["intents"].get(record.intent, 0) + 1
    stats["outcomes"][record.outcome] = stats["outcomes"].get(record.outcome, 0) + 1

    for question in record.key_questions:
        normalized = question.lower().strip()
        if normalized:
            stats["questions"][normalized] = stats["questions"].get(normalized, 0) + 1

    manager_stats = stats["managers"].setdefault(
        record.manager_id,
        {
            "manager_id": record.manager_id,
            "manager_name": record.manager_name,
            "role": record.role,
            "calls": 0,
        },
    )
    manager_stats["calls"] += 1


def _finalize_customer_stats(stats: dict[str, Any]) -> dict[str, Any]:
    result = dict(stats)

    managers = list(result.pop("managers").values())
    managers.sort(key=lambda item: (-item["calls"], item["manager_name"], item["manager_id"]))
    result["managers"] = managers

    result["top_intents"] = sorted(result.pop("intents").items(), key=lambda kv: kv[1], reverse=True)[:10]
    result["top_outcomes"] = sorted(result.pop("outcomes").items(), key=lambda kv: kv[1], reverse=True)[:5]
    result["top_questions"] = sorted(result.pop("questions").items(), key=lambda kv: kv[1], reverse=True)[:10]
    return result


def build_customers_report(
    source: ReportingSource,
    filters: ReportFilters,
    spam_threshold: float,
    sort_by: str = "total_calls",
    order: str = "desc",
) -> dict[str, Any]:
    customers_stats: dict[str, dict[str, Any]] = {}

    for record in source.iter_call_records(filters):
        if not _include_record(record, filters, spam_threshold):
            continue

        customer_phone, display_phone = _resolve_customer_phone(record)
        stats = customers_stats.setdefault(customer_phone, _customer_bucket(customer_phone, display_phone))
        if stats["display_phone"] == "unknown" and display_phone != "unknown":
            stats["display_phone"] = display_phone
        _accumulate_customer_stats(stats, record, spam_threshold)

    all_customers = [
        _finalize_customer_stats(stats)
        for _, stats in sorted(customers_stats.items(), key=lambda item: item[0])
    ]

    reverse = order == "desc"
    if sort_by in {"customer_phone", "display_phone", "first_call_date", "last_call_date"}:
        all_customers.sort(
            key=lambda item: (item.get(sort_by) or "", item["customer_phone"]),
            reverse=reverse,
        )
    else:
        all_customers.sort(
            key=lambda item: (
                item.get(sort_by, 0),
                item.get("last_call_date") or "",
                item["customer_phone"],
            ),
            reverse=reverse,
        )

    return {
        "generated_at": _utc_now_iso(),
        "data_source": source.source_name,
        "filters": filters.as_dict(),
        "skipped_metrics_available": False,
        "all_customers": all_customers,
        "total_customers": len(all_customers),
    }


def build_customer_followup_report(
    source: ReportingSource,
    filters: ReportFilters,
    spam_threshold: float,
    customer_phone: str,
) -> dict[str, Any] | None:
    target_phone = _normalize_phone_number(customer_phone)
    if not target_phone:
        return None

    customer_stats: dict[str, Any] | None = None
    calls: list[dict[str, Any]] = []

    for record in source.iter_call_records(filters):
        if not _include_record(record, filters, spam_threshold):
            continue

        normalized_phone, display_phone = _resolve_customer_phone(record)
        if normalized_phone != target_phone:
            continue

        if customer_stats is None:
            customer_stats = _customer_bucket(normalized_phone, display_phone)
        _accumulate_customer_stats(customer_stats, record, spam_threshold)

        calls.append(
            {
                "call_id": record.call_id,
                "call_date": record.call_date,
                "direction": record.direction,
                "manager_id": record.manager_id,
                "manager_name": record.manager_name,
                "role": record.role,
                "spam_probability": record.spam_probability,
                "effective_call": record.effective_call,
                "intent": record.intent,
                "outcome": record.outcome,
                "summary": record.summary,
                "audio_seconds": record.audio_seconds,
                "src_number": record.src_number,
                "dst_number": record.dst_number,
                "key_questions": list(record.key_questions),
                "objections": list(record.objections),
            }
        )

    if customer_stats is None:
        return None

    calls.sort(key=lambda item: ((item.get("call_date") or ""), item["call_id"]), reverse=True)
    report = _finalize_customer_stats(customer_stats)
    report["calls"] = calls
    report["generated_at"] = _utc_now_iso()
    report["data_source"] = source.source_name
    report["filters"] = filters.as_dict()
    report["skipped_metrics_available"] = False
    return report
