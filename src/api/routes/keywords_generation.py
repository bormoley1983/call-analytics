import logging
import os

from fastapi import APIRouter, HTTPException, status

from adapters.keywords_postgres import PostgresKeywordSource
from adapters.reporting_postgres import PostgresReportingSource
from api.schemas import KeywordGenerationPublishRequest, KeywordGenerationRequest
from core.keywords_generate import generate_keyword_candidates, publish_generated_keywords
from core.keywords_materialize import materialize_call_keywords
from domain.reporting import ReportFilters

router = APIRouter(prefix="/keywords/generation", tags=["keywords-generation"])
logger = logging.getLogger(__name__)


def _get_postgres_keyword_source() -> PostgresKeywordSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.warning("Keyword generation endpoint called without POSTGRES_DSN in process environment")
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword generation requires POSTGRES_DSN",
        )
    return PostgresKeywordSource(dsn)


def _get_postgres_reporting_source() -> PostgresReportingSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.warning("Keyword generation endpoint called without POSTGRES_DSN in process environment")
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword generation requires POSTGRES_DSN",
        )
    return PostgresReportingSource(dsn)


def _build_filters(req: KeywordGenerationRequest) -> ReportFilters:
    return ReportFilters(
        date_from=req.date_from,
        date_to=req.date_to,
        manager_id=req.manager_id,
        role=req.role,
        direction=req.direction,
        intent=req.intent,
        outcome=req.outcome,
        spam_only=req.spam_only,
        effective_only=req.effective_only,
    )


@router.post(
    "/candidates",
    summary="Generate keyword candidates from analyses",
    description=(
        "Scans existing Postgres analyses (`summary`, `key_questions`, `objections`) and returns ranked "
        "candidate phrases for keyword catalog creation.\n\n"
        "Default behavior is broad but quality-oriented:\n"
        "- no date filters unless you provide them\n"
        "- `effective_only=true` by default\n"
        "- existing keyword terms are excluded by default\n\n"
        "This endpoint does not modify catalog data."
    ),
    responses={
        405: {
            "description": "Generation requires Postgres.",
            "content": {"application/json": {"example": {"detail": "Keyword generation requires POSTGRES_DSN"}}},
        },
    },
)
def generate_candidates(req: KeywordGenerationRequest):
    reporting_source = _get_postgres_reporting_source()
    keyword_source = _get_postgres_keyword_source()
    try:
        data = generate_keyword_candidates(
            reporting_source=reporting_source,
            keyword_source=keyword_source,
            filters=_build_filters(req),
            include_summary=req.include_summary,
            include_key_questions=req.include_key_questions,
            include_objections=req.include_objections,
            min_token_length=req.min_token_length,
            max_ngram_words=req.max_ngram_words,
            min_support_calls=req.min_support_calls,
            min_total_matches=req.min_total_matches,
            max_candidates=req.max_candidates,
            exclude_existing_terms=req.exclude_existing_terms,
            spam_threshold=float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7")),
        )
        data["filters"] = _build_filters(req).as_dict()
        return data
    finally:
        reporting_source.close()
        keyword_source.close()


@router.post(
    "/publish",
    summary="Publish generated candidates to keyword catalog",
    description=(
        "Creates/updates keyword catalog entries from generated candidate phrases.\n\n"
        "Optionally runs immediate materialization after publish."
    ),
    responses={
        405: {
            "description": "Publish requires Postgres.",
            "content": {"application/json": {"example": {"detail": "Keyword generation requires POSTGRES_DSN"}}},
        },
    },
)
def publish_candidates(req: KeywordGenerationPublishRequest):
    keyword_source = _get_postgres_keyword_source()
    reporting_source = _get_postgres_reporting_source() if req.materialize_after_publish else None
    try:
        publish_result = publish_generated_keywords(
            keyword_source=keyword_source,
            candidates=[item.model_dump(exclude_none=True) for item in req.candidates],
            default_category=req.default_category,
            default_match_fields=req.default_match_fields,
            default_is_active=req.default_is_active,
        )

        response = {
            "publish": publish_result,
            "materialized": False,
        }
        has_changes = publish_result["created"] + publish_result["updated"] > 0
        if req.materialize_after_publish and reporting_source is not None and has_changes:
            response["materialize"] = materialize_call_keywords(
                reporting_source=reporting_source,
                keyword_source=keyword_source,
                keyword_store=keyword_source,
                state_store=keyword_source,
            )
            response["materialized"] = True
        return response
    finally:
        if reporting_source is not None:
            reporting_source.close()
        keyword_source.close()
