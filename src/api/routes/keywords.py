import logging
import os
import re
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Path, status

from adapters.keywords_postgres import PostgresKeywordSource
from adapters.reporting_postgres import PostgresReportingSource
from adapters.keywords_yaml import YamlKeywordSource
from api.schemas import KeywordSyncRequest, KeywordUpsertRequest
from core.keywords_ai_runtime import run_keyword_ai_analysis_once
from core.keywords_refresh import refresh_keywords_data
from core.keywords_service import list_keywords
from core.keywords_materialize import materialize_call_keywords
from core.keywords_sync import sync_keywords_to_postgres
from domain.config import KEYWORDS_CONFIG
from domain.keywords import KeywordDefinition

router = APIRouter(prefix="/keywords", tags=["keywords"])
logger = logging.getLogger(__name__)

_SAFE_ID = re.compile(r"^[\w\-]+$")
_SAFE_ID_PATTERN = r"^[\w\-]+$"


def _get_keyword_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresKeywordSource(dsn)
    source = YamlKeywordSource(KEYWORDS_CONFIG, strict=True)
    try:
        list(source.list_keywords())
    except (FileNotFoundError, ValueError) as exc:
        source.close()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return source


def _get_yaml_keyword_source(*, strict: bool = False) -> YamlKeywordSource:
    return YamlKeywordSource(KEYWORDS_CONFIG, strict=strict)


def _append_keyword_ai_analysis(result: dict[str, Any], *, trigger: str) -> dict[str, Any]:
    try:
        keyword_ai_analysis = run_keyword_ai_analysis_once(trigger)
    except Exception as exc:
        logger.exception("AI keyword analysis failed after %s", trigger)
        result["keyword_ai_analysis_error"] = str(exc)
    else:
        if keyword_ai_analysis is not None:
            result["keyword_ai_analysis"] = keyword_ai_analysis
    return result


def _get_writable_keyword_source() -> PostgresKeywordSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.warning("Keyword write endpoint called without POSTGRES_DSN in process environment")
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword catalog is read-only without POSTGRES_DSN",
        )
    return PostgresKeywordSource(dsn)


def _get_postgres_reporting_source() -> PostgresReportingSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.warning("Keyword materialization endpoint called without POSTGRES_DSN in process environment")
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword materialization requires POSTGRES_DSN",
        )
    return PostgresReportingSource(dsn)


@router.get(
    "",
    summary="List keyword catalog",
    description=(
        "Returns keyword definitions used for reporting.\n\n"
        "Read source is selected automatically:\n"
        "- Postgres when `POSTGRES_DSN` is configured\n"
        "- YAML file otherwise"
    ),
)
def keywords_catalog():
    source = _get_keyword_source()
    try:
        return list_keywords(source)
    finally:
        source.close()


@router.post(
    "/refresh",
    summary="Refresh keywords for reporting",
    description=(
        "Runs the full keyword refresh flow in Postgres.\n\n"
        "This endpoint:\n"
        "- syncs keyword definitions from YAML into Postgres\n"
        "- materializes keyword-to-call matches from existing analyses\n\n"
        "Use this as the main manual keyword preparation endpoint. Normally this also runs automatically "
        "after successful processing jobs when `POSTGRES_DSN` is configured."
    ),
    responses={
        400: {
            "description": "Invalid keyword source data.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword source data"}}},
        },
        405: {
            "description": "Refresh requires Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword materialization requires POSTGRES_DSN"}}
            },
        },
    },
)
def refresh_keywords(req: KeywordSyncRequest | None = None):
    options = req or KeywordSyncRequest()
    postgres_source = _get_writable_keyword_source()
    yaml_source = _get_yaml_keyword_source(strict=True)
    reporting_source = _get_postgres_reporting_source()
    try:
        try:
            result = refresh_keywords_data(
                yaml_source=yaml_source,
                postgres_source=postgres_source,
                reporting_source=reporting_source,
                prune_missing=options.prune_missing,
            )
            return _append_keyword_ai_analysis(result, trigger="keywords-refresh")
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        reporting_source.close()
        yaml_source.close()
        postgres_source.close()


@router.post(
    "/sync",
    summary="Sync YAML keywords to Postgres",
    description=(
        "Copies keyword definitions from YAML config into Postgres.\n\n"
        "**Default**: `prune_missing=false` (existing Postgres-only rows are kept).\n\n"
        "Low-level maintenance endpoint. For normal manual use prefer `POST /keywords/refresh`."
    ),
    responses={
        400: {
            "description": "Invalid sync source data.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword source data"}}},
        },
        405: {
            "description": "Write operations require Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword catalog is read-only without POSTGRES_DSN"}}
            },
        },
    },
)
def sync_keywords(req: KeywordSyncRequest):
    postgres_source = _get_writable_keyword_source()
    yaml_source = _get_yaml_keyword_source(strict=True)
    try:
        try:
            result = sync_keywords_to_postgres(
                yaml_source=yaml_source,
                postgres_source=postgres_source,
                prune_missing=req.prune_missing,
            )
            return _append_keyword_ai_analysis(result, trigger="keywords-sync")
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        yaml_source.close()
        postgres_source.close()


@router.post(
    "/materialize",
    summary="Materialize keyword matches",
    description=(
        "Builds/refreshes materialized keyword-to-call matches in Postgres.\n\n"
        "Required for report drill-down endpoints under `/reports/keywords/{keyword_id}/...`.\n\n"
        "Low-level maintenance endpoint. For normal manual use prefer `POST /keywords/refresh`."
    ),
    responses={
        405: {
            "description": "Materialization requires Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword materialization requires POSTGRES_DSN"}}
            },
        }
    },
)
def materialize_keywords():
    reporting_source = _get_postgres_reporting_source()
    keyword_source = _get_writable_keyword_source()
    try:
        result = materialize_call_keywords(
            reporting_source=reporting_source,
            keyword_source=keyword_source,
            keyword_store=keyword_source,
            state_store=keyword_source,
        )
        return _append_keyword_ai_analysis(result, trigger="keywords-materialize")
    finally:
        reporting_source.close()
        keyword_source.close()


@router.get(
    "/{keyword_id}",
    summary="Get keyword by id",
    responses={
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword not found.",
            "content": {"application/json": {"example": {"detail": "Keyword not found"}}},
        },
    },
)
def keyword_detail(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            examples=["delivery"],
        ),
    ]
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    source = _get_keyword_source()
    try:
        for keyword in list_keywords(source)["keywords"]:
            if keyword["keyword_id"] == keyword_id:
                return keyword
    finally:
        source.close()
    raise HTTPException(status_code=404, detail="Keyword not found")


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create keyword",
    description="Creates a new keyword definition in Postgres.",
    responses={
        405: {
            "description": "Create requires Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword catalog is read-only without POSTGRES_DSN"}}
            },
        },
        409: {
            "description": "Keyword already exists.",
            "content": {"application/json": {"example": {"detail": "Keyword already exists"}}},
        },
    },
)
def create_keyword(req: KeywordUpsertRequest):
    source = _get_writable_keyword_source()
    try:
        existing = source.get_keyword(req.keyword_id)
        if existing is not None:
            raise HTTPException(status_code=409, detail="Keyword already exists")
        created = source.upsert_keyword(
            KeywordDefinition(
                keyword_id=req.keyword_id,
                label=req.label,
                category=req.category,
                terms=req.terms,
                match_fields=req.match_fields,
                is_active=req.is_active,
            )
        )
        return {
            "keyword_id": created.keyword_id,
            "label": created.label,
            "category": created.category,
            "terms": created.terms,
            "match_fields": created.match_fields,
            "is_active": created.is_active,
        }
    finally:
        source.close()


@router.put(
    "/{keyword_id}",
    summary="Update keyword",
    description="Updates an existing keyword in Postgres. Path id must match body `keyword_id`.",
    responses={
        400: {
            "description": "Invalid id or path/body mismatch.",
            "content": {
                "application/json": {"example": {"detail": "Path keyword_id must match request body"}}
            },
        },
        404: {
            "description": "Keyword not found.",
            "content": {"application/json": {"example": {"detail": "Keyword not found"}}},
        },
        405: {
            "description": "Update requires Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword catalog is read-only without POSTGRES_DSN"}}
            },
        },
    },
)
def update_keyword(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            examples=["delivery"],
        ),
    ],
    req: KeywordUpsertRequest,
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    if req.keyword_id != keyword_id:
        raise HTTPException(status_code=400, detail="Path keyword_id must match request body")
    source = _get_writable_keyword_source()
    try:
        existing = source.get_keyword(keyword_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Keyword not found")
        updated = source.upsert_keyword(
            KeywordDefinition(
                keyword_id=req.keyword_id,
                label=req.label,
                category=req.category,
                terms=req.terms,
                match_fields=req.match_fields,
                is_active=req.is_active,
            )
        )
        return {
            "keyword_id": updated.keyword_id,
            "label": updated.label,
            "category": updated.category,
            "terms": updated.terms,
            "match_fields": updated.match_fields,
            "is_active": updated.is_active,
        }
    finally:
        source.close()


@router.delete(
    "/{keyword_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete keyword",
    description="Deletes a keyword definition from Postgres.",
    responses={
        400: {
            "description": "Invalid keyword id format.",
            "content": {"application/json": {"example": {"detail": "Invalid keyword_id"}}},
        },
        404: {
            "description": "Keyword not found.",
            "content": {"application/json": {"example": {"detail": "Keyword not found"}}},
        },
        405: {
            "description": "Delete requires Postgres.",
            "content": {
                "application/json": {"example": {"detail": "Keyword catalog is read-only without POSTGRES_DSN"}}
            },
        },
    },
)
def delete_keyword(
    keyword_id: Annotated[
        str,
        Path(
            description="Keyword identifier (letters, digits, underscore, dash).",
            pattern=_SAFE_ID_PATTERN,
            examples=["delivery"],
        ),
    ]
):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    source = _get_writable_keyword_source()
    try:
        deleted = source.delete_keyword(keyword_id)
    finally:
        source.close()
    if not deleted:
        raise HTTPException(status_code=404, detail="Keyword not found")
