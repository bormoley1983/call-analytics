from __future__ import annotations

import logging
import os
from typing import Any

from adapters.keyword_ai_analysis_postgres import PostgresKeywordAiAnalysisStore
from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from adapters.llm_ollama import OllamaLlm
from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from core.keywords_ai import run_keyword_catalog_analysis
from domain.config import ANALYSIS, KEYWORDS_CONFIG, load_app_config
from ports.keywords import KeywordSource
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)


def auto_keyword_ai_analysis_enabled() -> bool:
    return os.getenv("AUTO_RUN_AI_KEYWORD_ANALYSIS", "1") != "0"


def run_keyword_ai_analysis_once(trigger: str) -> dict[str, Any] | None:
    if not auto_keyword_ai_analysis_enabled():
        logger.info("Skipping AI keyword analysis because AUTO_RUN_AI_KEYWORD_ANALYSIS=0")
        return None

    dsn = os.getenv("POSTGRES_DSN")
    config = load_app_config()
    llm = OllamaLlm(config)
    keyword_source: KeywordSource
    reporting_source: ReportingSource
    analysis_store: PostgresKeywordAiAnalysisStore | None
    if dsn:
        logger.info("Running AI keyword analysis after %s using Postgres sources", trigger)
        keyword_source = PostgresKeywordSource(dsn)
        reporting_source = PostgresReportingSource(dsn)
        analysis_store = PostgresKeywordAiAnalysisStore(dsn)
    else:
        logger.info("Running AI keyword analysis after %s using JSON/YAML sources", trigger)
        keyword_source = YamlKeywordSource(KEYWORDS_CONFIG, strict=True)
        reporting_source = JsonReportingSource(ANALYSIS)
        analysis_store = None

    try:
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
            spam_threshold=float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7")),
            ai_model=getattr(config, "ollama_model", None),
        )
    finally:
        reporting_source.close()
        keyword_source.close()
        if analysis_store is not None:
            analysis_store.close()
