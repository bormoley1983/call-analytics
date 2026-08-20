"""Composition-root dependency factories for the API edge.

These helpers own driver selection (Postgres vs JSON/YAML fallback) so that
core modules never import concrete adapters or read ``POSTGRES_DSN``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, status

from adapters.keyword_ai_analysis_postgres import PostgresKeywordAiAnalysisStore
from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from adapters.llm_ollama import OllamaLlm
from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from domain.config import (
    get_analysis_dir,
    get_keywords_config,
    get_postgres_dsn as _config_get_postgres_dsn,
    get_spam_probability_threshold,
    load_app_config,
)
from ports.keywords import KeywordSource
from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)


def get_postgres_dsn() -> str | None:
    """Single place that reads POSTGRES_DSN for the API edge."""
    return _config_get_postgres_dsn()


def require_postgres_dsn(*, detail: str) -> str:
    """Raise a 405 with `detail` when POSTGRES_DSN is not configured.

    Replaces the per-route ``dsn = os.getenv(...)`` + HTTPException blocks that
    were duplicated across keywords/keywords_ai/keywords_generation/reports.
    """
    dsn = get_postgres_dsn()
    if not dsn:
        logger.warning("Endpoint called without POSTGRES_DSN in process environment")
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail=detail,
        )
    return dsn


def get_keyword_source() -> KeywordSource:
    """Postgres keyword source when DSN is set, YAML fallback otherwise.

    The YAML fallback is validated eagerly (fail loudly with a 500 on a
    missing or invalid keywords.yaml) — this replaces the per-route validation
    blocks that were copy-pasted in keywords.py / keywords_ai.py / reports.py.
    """
    dsn = get_postgres_dsn()
    if dsn:
        return PostgresKeywordSource(dsn)
    source = YamlKeywordSource(get_keywords_config(), strict=True)
    try:
        list(source.list_keywords())
    except (FileNotFoundError, ValueError) as exc:
        source.close()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        ) from exc
    return source


def get_reporting_source() -> ReportingSource:
    """Postgres reporting source when DSN is set, JSON fallback otherwise."""
    dsn = get_postgres_dsn()
    if dsn:
        return PostgresReportingSource(dsn)
    return JsonReportingSource(get_analysis_dir())


def get_keyword_ai_analysis_store() -> PostgresKeywordAiAnalysisStore | None:
    """Postgres analysis store, or None when DSN is not configured."""
    dsn = get_postgres_dsn()
    if not dsn:
        return None
    return PostgresKeywordAiAnalysisStore(dsn)


def get_llm() -> LlmPort:
    config = load_app_config()
    return OllamaLlm(config)


def keyword_source_factory() -> Callable[[], KeywordSource]:
    """Return a zero-arg factory that builds the driver-selected keyword source.

    Routes use this instead of duplicating the DSN check + YAML fallback
    (previously copy-pasted in keywords.py, keywords_ai.py and reports.py).
    """
    return get_keyword_source


def reporting_source_factory() -> Callable[[], ReportingSource]:
    """Return a zero-arg factory that builds the driver-selected reporting source.

    Routes use this instead of duplicating the DSN check + JSON fallback.
    """
    return get_reporting_source


@dataclass(frozen=True)
class KeywordAiRuntimeDeps:
    """Dependencies for one keyword AI analysis run."""

    keyword_source: KeywordSource
    reporting_source: ReportingSource
    llm: LlmPort
    analysis_store: Any | None
    spam_threshold: float
    ai_model: str | None


def build_keyword_ai_runtime_deps() -> KeywordAiRuntimeDeps:
    """Select concrete drivers for a keyword AI analysis run."""
    dsn = get_postgres_dsn()
    config = load_app_config()
    if dsn:
        logger.info("Keyword AI analysis using Postgres sources")
        return KeywordAiRuntimeDeps(
            keyword_source=PostgresKeywordSource(dsn),
            reporting_source=PostgresReportingSource(dsn),
            llm=OllamaLlm(config),
            analysis_store=PostgresKeywordAiAnalysisStore(dsn),
            spam_threshold=get_spam_probability_threshold(),
            ai_model=getattr(config, "ollama_model", None),
        )
    logger.info("Keyword AI analysis using JSON/YAML sources")
    return KeywordAiRuntimeDeps(
        keyword_source=YamlKeywordSource(get_keywords_config(), strict=True),
        reporting_source=JsonReportingSource(get_analysis_dir()),
        llm=OllamaLlm(config),
        analysis_store=None,
        spam_threshold=get_spam_probability_threshold(),
        ai_model=getattr(config, "ollama_model", None),
    )


def keyword_ai_analysis_factory() -> Callable[..., dict[str, Any] | None]:
    """Return a ``run_keyword_ai_analysis_once``-compatible callable.

    The returned function builds its own dependencies per call (driver
    selection happens at call time, not import time).
    """
    from core.keywords_ai_runtime import run_keyword_ai_analysis_once

    def _run(trigger: str, *, skip_if_empty: bool = False) -> dict[str, Any] | None:
        deps = build_keyword_ai_runtime_deps()
        try:
            return run_keyword_ai_analysis_once(
                trigger,
                keyword_source=deps.keyword_source,
                reporting_source=deps.reporting_source,
                llm=deps.llm,
                analysis_store=deps.analysis_store,
                skip_if_empty=skip_if_empty,
                spam_threshold=deps.spam_threshold,
                ai_model=deps.ai_model,
            )
        finally:
            for _source in (deps.reporting_source, deps.keyword_source):
                try:
                    _source.close()
                except Exception:
                    logger.exception("Error closing source during AI keyword analysis cleanup")
            if deps.analysis_store is not None:
                try:
                    deps.analysis_store.close()
                except Exception:
                    logger.exception("Error closing analysis store during AI keyword analysis cleanup")

    return _run
