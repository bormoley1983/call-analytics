from __future__ import annotations

import logging
import uuid
from typing import Any

from domain.reporting import ReportFilters
from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)

VALID_INSIGHT_TYPES = {"pain_points", "objections", "trends", "follow_up_risk"}


def _validate_record_quality(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate that analysis records have sufficient content for meaningful insights.

    A record is considered "usable" if it has at least one non-empty field
    (summary, key_questions, or objections). The check fires a warning when
    fewer than 5 records are usable — too few for the LLM to extract patterns.

    Returns a dict with usable_records count and any warnings.
    """
    warnings: list[str] = []
    total = len(records)
    usable = 0
    empty_all = 0

    for rec in records:
        summary = (rec.get("summary") or "").strip()
        questions = rec.get("key_questions") or []
        objections = rec.get("objections") or []

        if summary or questions or objections:
            usable += 1
        else:
            empty_all += 1

    if empty_all > total * 0.5:
        warnings.append(f"{empty_all}/{total} records have no content at all")

    return {
        "usable_records": usable,
        "warnings": warnings,
    }


def generate_deep_insights(
    reporting_source: ReportingSource,
    llm: LlmPort,
    *,
    insight_types: list[str],
    filters: Any | None = None,
    max_insights: int = 20,
) -> dict[str, Any]:
    """Generate deep business insights from analysis records.

    Fetches analysis records matching filters, calls LLM per insight type,
    and returns structured results.
    """
    # Validate insight types
    for it in insight_types:
        if it not in VALID_INSIGHT_TYPES:
            raise ValueError(
                f"Invalid insight type: {it}. Must be one of {VALID_INSIGHT_TYPES}"
            )

    # Collect analysis records as dicts for LLM consumption.
    # LLM prompt templates cap at 100 records, so we don't collect more than that.
    analysis_records = _collect_analysis_records(
        reporting_source, filters, max_records=100
    )

    if not analysis_records:
        logger.warning("No analysis records available for deep insights")
        return {
            "run_id": str(uuid.uuid4()),
            "insight_counts": {it: 0 for it in insight_types},
            "insights": [],
        }

    quality = _validate_record_quality(analysis_records)
    if quality["usable_records"] == 0 and quality["warnings"]:
        logger.warning(
            "Insufficient data quality for deep insights: %d/%d records have "
            "meaningful content (%s). Skipping LLM calls.",
            quality["usable_records"],
            len(analysis_records),
            ", ".join(quality["warnings"]),
        )
        return {
            "run_id": str(uuid.uuid4()),
            "insight_counts": {it: 0 for it in insight_types},
            "insights": [],
            "quality_warnings": quality["warnings"],
        }

    logger.info(
        "Generating deep insights: types=%s records=%d max_insights=%d",
        insight_types,
        len(analysis_records),
        max_insights,
    )

    all_insights: list[dict[str, Any]] = []
    insight_counts: dict[str, int] = {}

    for insight_type in insight_types:
        try:
            llm_result = llm.generate_deep_insights(insight_type, analysis_records)
            insights = llm_result.get("insights", [])[:max_insights]

            # Tag each insight with its type
            for ins in insights:
                ins["insight_type"] = insight_type

            all_insights.extend(insights)
            insight_counts[insight_type] = len(insights)

            logger.info(
                "Generated %d insights for type=%s",
                len(insights),
                insight_type,
            )
        except (RuntimeError, TypeError) as exc:
            logger.error(
                "Failed to generate insights for type=%s: %s", insight_type, exc
            )
            insight_counts[insight_type] = 0

    run_id = str(uuid.uuid4())

    return {
        "run_id": run_id,
        "insight_counts": insight_counts,
        "insights": all_insights,
    }


def _collect_analysis_records(
    reporting_source: ReportingSource,
    filters: Any | None,
    max_records: int = 100,
) -> list[dict[str, Any]]:
    """Collect analysis records as dicts suitable for LLM prompts.

    Args:
        reporting_source: Source of call records.
        filters: Date/manager/role filters (dict, ReportFilters, or None).
        max_records: Maximum number of records to collect (default 100,
            aligned with the LLM prompt template limit).
    """
    # Normalize dict filters to ReportFilters
    if isinstance(filters, dict):
        rf = ReportFilters(
            date_from=filters.get("date_from"),
            date_to=filters.get("date_to"),
            manager_id=filters.get("manager_id"),
            role=filters.get("role"),
        )
    elif filters is None:
        rf = ReportFilters()
    else:
        rf = filters

    records: list[dict[str, Any]] = []

    for record in reporting_source.iter_call_records(rf):
        records.append(
            {
                "call_id": record.call_id,
                "manager_id": record.manager_id,
                "role": record.role,
                "direction": record.direction,
                "intent": record.intent,
                "outcome": record.outcome,
                "effective_call": record.effective_call,
                "spam_probability": record.spam_probability,
                "summary": record.summary or "",
                "key_questions": (
                    list(record.key_questions) if record.key_questions else []
                ),
                "objections": list(record.objections) if record.objections else [],
                "call_datetime": (
                    record.call_datetime.isoformat() if record.call_datetime else None
                ),
            }
        )

    return records[:max_records]


def store_deep_insights_run(
    store: Any,
    run_data: dict[str, Any],
    *,
    ai_model: str | None = None,
    filters: Any | None = None,
    insight_types: list[str] | None = None,
) -> str:
    """Store a deep insights run in the Postgres store.

    Returns the run_id.
    """
    run_id = run_data.get("run_id", str(uuid.uuid4()))

    # Build request data
    request_data: dict[str, Any] = {
        "insight_types": insight_types or [],
        "max_insights": run_data.get("max_insights", 20),
    }
    if filters:
        if hasattr(filters, "as_dict"):
            request_data["filters"] = filters.as_dict()
        else:
            request_data["filters"] = filters or {}

    store.create_run(
        run_id=run_id,
        ai_model=ai_model,
        insight_types=insight_types or [],
        request_data=request_data,
    )

    # Store individual insights
    insights = run_data.get("insights", [])
    if insights:
        store.add_insights(run_id, insights)

    logger.info(
        "Stored deep insights run: run_id=%s total_insights=%d",
        run_id,
        len(insights),
    )
    return run_id
