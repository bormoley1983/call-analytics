"""Runtime orchestration for keyword AI analysis.

Driver selection (Postgres vs JSON/YAML) is owned by the edge layer
(``api/deps.py``); this module only receives already-constructed sources and
delegates to :func:`core.keywords_ai.run_keyword_catalog_analysis`.
"""

from __future__ import annotations

import logging
from typing import Any

from core.keywords_ai import run_keyword_catalog_analysis
from ports.keywords import KeywordSource
from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)


def auto_keyword_ai_analysis_enabled() -> bool:
    from domain.config import get_auto_run_ai_keyword_analysis

    return get_auto_run_ai_keyword_analysis()


def _has_keywords_for_analysis(keyword_source: KeywordSource) -> bool:
    return any(
        keyword.is_active and bool(keyword.terms)
        for keyword in keyword_source.list_keywords()
    )


def run_keyword_ai_analysis_once(
    trigger: str,
    *,
    keyword_source: KeywordSource,
    reporting_source: ReportingSource,
    llm: LlmPort,
    analysis_store: Any | None = None,
    skip_if_empty: bool = False,
    spam_threshold: float = 0.7,
    ai_model: str | None = None,
) -> dict[str, Any] | None:
    """Run one keyword catalog AI analysis using the provided sources.

    Callers are responsible for closing the sources after the call (the edge
    factory in ``api/deps.py`` does this).
    """
    if not auto_keyword_ai_analysis_enabled():
        logger.info("Skipping AI keyword analysis because AUTO_RUN_AI_KEYWORD_ANALYSIS=0")
        return None

    if skip_if_empty and not _has_keywords_for_analysis(keyword_source):
        logger.info(
            "Skipping AI keyword analysis after %s because the keyword catalog has no active keywords",
            trigger,
        )
        return None

    return run_keyword_catalog_analysis(
        request_data={
            "trigger": trigger,
            "include_inactive": False,
            "include_match_stats": True,
            "keyword_ids": None,
            "max_keywords": 100,
            "max_groups": 20,
        },
        keyword_source=keyword_source,
        reporting_source=reporting_source,
        llm=llm,
        analysis_store=analysis_store,
        include_inactive=False,
        include_match_stats=True,
        max_keywords=100,
        max_groups=20,
        spam_threshold=spam_threshold,
        ai_model=ai_model,
    )
