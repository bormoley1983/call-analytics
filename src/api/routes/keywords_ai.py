import logging
import os
from typing import List

from fastapi import APIRouter, HTTPException, Path, Query, status

from adapters.keyword_ai_analysis_postgres import PostgresKeywordAiAnalysisStore
from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from adapters.llm_ollama import OllamaLlm
from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from api.schemas import (
    AIApplyAction,
    AIApplyDryRunResult,
    AIApplyHistoryEntry,
    AIApplyRequest,
    AIApplyResult,
    KeywordCatalogAnalysisRequest,
)
from core.keywords_ai import run_keyword_catalog_analysis
from domain.config import ANALYSIS, KEYWORDS_CONFIG, load_app_config

router = APIRouter(prefix="/keywords/catalog", tags=["keywords-ai"])
logger = logging.getLogger(__name__)
_SAFE_ANALYSIS_ID_PATTERN = r"^[0-9a-fA-F\-]{36}$"


def _get_keyword_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresKeywordSource(dsn)
    source = YamlKeywordSource(KEYWORDS_CONFIG, strict=True)
    try:
        list(source.list_keywords())
    except (FileNotFoundError, ValueError) as exc:
        source.close()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    return source


def _get_reporting_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresReportingSource(dsn)
    return JsonReportingSource(ANALYSIS)


def _get_keyword_ai_analysis_store() -> PostgresKeywordAiAnalysisStore | None:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        return None
    return PostgresKeywordAiAnalysisStore(dsn)


def _get_required_keyword_ai_analysis_store() -> PostgresKeywordAiAnalysisStore:
    store = _get_keyword_ai_analysis_store()
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword AI analysis history requires POSTGRES_DSN",
        )
    return store


def _execute_keyword_catalog_analysis(req: KeywordCatalogAnalysisRequest):
    keyword_source = _get_keyword_source()
    reporting_source = _get_reporting_source() if req.include_match_stats else None
    analysis_store = _get_keyword_ai_analysis_store()
    try:
        config = load_app_config()
        llm = OllamaLlm(config)
        return run_keyword_catalog_analysis(
            request_data=req.model_dump(mode="json"),
            keyword_source=keyword_source,
            reporting_source=reporting_source,
            llm=llm,
            analysis_store=analysis_store,
            include_inactive=req.include_inactive,
            include_match_stats=req.include_match_stats,
            keyword_ids=req.keyword_ids,
            max_keywords=req.max_keywords,
            max_groups=req.max_groups,
            spam_threshold=float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7")),
            ai_model=getattr(config, "ollama_model", None),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Keyword AI analysis failed")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Keyword AI analysis failed: {exc}",
        ) from exc
    finally:
        if reporting_source is not None:
            reporting_source.close()
        keyword_source.close()
        if analysis_store is not None:
            analysis_store.close()


@router.post(
    "/analysis",
    summary="AI analysis of keyword catalog",
    description=(
        "Uses AI to analyze the existing keyword catalog, group related keywords, and suggest safe cleanup actions.\n\n"
        "This endpoint is advisory only. It does not mutate the keyword catalog.\n\n"
        "When `POSTGRES_DSN` is configured, each analysis run is also persisted into Postgres analysis history."
    ),
    responses={
        502: {
            "description": "AI analysis failed.",
            "content": {
                "application/json": {
                    "example": {"detail": "Keyword AI analysis failed: ..."}
                }
            },
        },
    },
)
def analyze_keyword_catalog(req: KeywordCatalogAnalysisRequest):
    return _execute_keyword_catalog_analysis(req)


@router.get(
    "/analyses",
    summary="List persisted AI keyword analyses",
    description="Returns persisted AI keyword catalog analysis runs stored in Postgres.",
    responses={
        405: {
            "description": "Analysis history requires Postgres.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Keyword AI analysis history requires POSTGRES_DSN"
                    }
                }
            },
        },
    },
)
def list_keyword_analyses(
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of persisted analysis runs to return.",
    ),
):
    store = _get_required_keyword_ai_analysis_store()
    try:
        analyses = store.list_analyses(limit=limit)
        return {
            "returned": len(analyses),
            "analyses": analyses,
        }
    finally:
        store.close()


@router.get(
    "/analyses/{analysis_id}",
    summary="Get persisted AI keyword analysis",
    description="Returns one persisted AI keyword catalog analysis from Postgres, including stored analysis items.",
    responses={
        404: {
            "description": "Analysis id not found.",
            "content": {
                "application/json": {
                    "example": {"detail": "Keyword AI analysis not found"}
                }
            },
        },
        405: {
            "description": "Analysis history requires Postgres.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Keyword AI analysis history requires POSTGRES_DSN"
                    }
                }
            },
        },
    },
)
def get_keyword_analysis(
    analysis_id: str = Path(
        description="Persisted keyword AI analysis identifier.",
        pattern=_SAFE_ANALYSIS_ID_PATTERN,
        examples=["11111111-1111-1111-1111-111111111111"],
    ),
):
    store = _get_required_keyword_ai_analysis_store()
    try:
        analysis = store.get_analysis(analysis_id)
    finally:
        store.close()
    if analysis is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Keyword AI analysis not found",
        )
    return analysis


def _get_apply_store() -> "PostgresAiApplyStore":
    from adapters.ai_apply_postgres import PostgresAiApplyStore

    cfg = load_app_config()
    dsn = os.getenv("POSTGRES_DSN") or (cfg.get("postgres") or {}).get("dsn", "")
    if not dsn:
        raise HTTPException(status_code=400, detail="No POSTGRES_DSN configured")
    return PostgresAiApplyStore(dsn)


@router.post(
    "/analyses/{analysis_id}/apply",
    response_model=AIApplyResult,
    status_code=status.HTTP_201_CREATED,
    summary="Apply approved catalog actions",
)
def apply_analysis_actions(
    analysis_id: str,
    request: AIApplyRequest,
) -> AIApplyResult:
    """Apply selected actions from a persisted analysis.

    The request can specify actions by group_index/action_index or by keyword_id.
    In dry_run mode, mutations are previewed but not executed.
    """
    analysis_store = _get_required_keyword_ai_analysis_store()
    apply_store = _get_apply_store()
    keyword_source = _get_keyword_source()

    try:
        # Fetch analysis
        analysis = analysis_store.get_analysis(analysis_id)
        if analysis is None:
            raise HTTPException(status_code=404, detail="Analysis not found")

        # Import here to avoid circular imports at module load time
        from core.ai_apply import apply_approved_actions

        result = apply_approved_actions(
            analysis_id=analysis_id,
            analysis=analysis,
            request_actions=request.actions,
            keyword_source=keyword_source,
            apply_store=apply_store,
            dry_run=request.dry_run,
            refresh_after=request.refresh_after,
            applied_by="api",
        )
    finally:
        analysis_store.close()
        apply_store.close()
        keyword_source.close()

    return AIApplyResult(**result)


@router.get(
    "/analyses/{analysis_id}/apply/history",
    response_model=List[AIApplyHistoryEntry],
    summary="Get apply history for an analysis",
)
def get_analysis_apply_history(
    analysis_id: str = Path(..., pattern=_SAFE_ANALYSIS_ID_PATTERN),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> List[AIApplyHistoryEntry]:
    """Get apply history for a specific analysis."""
    from core.ai_apply import get_apply_history

    apply_store = _get_apply_store()
    try:
        raw_records = get_apply_history(
            analysis_id, apply_store, limit=limit, offset=offset
        )
    finally:
        apply_store.close()

    entries: List[AIApplyHistoryEntry] = []
    for rec in raw_records:
        actions_applied = rec.get("actions_applied") or []
        actions_skipped = rec.get("actions_skipped") or []
        mutations = rec.get("mutations") or []
        entries.append(
            AIApplyHistoryEntry(
                apply_id=str(rec["apply_id"]),
                applied_at=str(rec["applied_at"]),
                applied_by=rec.get("applied_by"),
                dry_run=bool(rec["dry_run"]),
                actions_count=len(actions_applied) + len(actions_skipped),
                mutations_count=len(mutations),
                keyword_refreshed=bool(rec.get("keyword_refreshed", False)),
                follow_up_ran=bool(rec.get("follow_up_ran", False)),
                error=rec.get("error"),
            )
        )
    return entries
