from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from domain.reporting import ReportFilters
from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)

VALID_INSIGHT_TYPES = {"pain_points", "objections", "trends", "follow_up_risk"}


def generate_deep_insights(
    reporting_source: ReportingSource,
    llm: LlmPort,
    *,
    insight_types: List[str],
    filters: Any | None = None,
    max_insights: int = 20,
) -> Dict[str, Any]:
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

    # Collect analysis records as dicts for LLM consumption
    analysis_records = _collect_analysis_records(reporting_source, filters)

    if not analysis_records:
        logger.warning("No analysis records available for deep insights")
        return {
            "run_id": str(uuid.uuid4()),
            "insight_counts": {it: 0 for it in insight_types},
            "insights": [],
        }

    logger.info(
        "Generating deep insights: types=%s records=%d max_insights=%d",
        insight_types,
        len(analysis_records),
        max_insights,
    )

    all_insights: List[Dict[str, Any]] = []
    insight_counts: Dict[str, int] = {}

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
        except Exception as exc:
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
) -> List[Dict[str, Any]]:
    """Collect analysis records as dicts suitable for LLM prompts."""
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

    records: List[Dict[str, Any]] = []

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

    return records[:200]  # Cap at 200 records for LLM context window


def store_deep_insights_run(
    store: Any,
    run_data: Dict[str, Any],
    *,
    ai_model: str | None = None,
    filters: Any | None = None,
    insight_types: List[str] | None = None,
) -> str:
    """Store a deep insights run in the Postgres store.

    Returns the run_id.
    """
    run_id = run_data.get("run_id", str(uuid.uuid4()))

    # Build request data
    request_data: Dict[str, Any] = {
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
