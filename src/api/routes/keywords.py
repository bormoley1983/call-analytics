import os
import re
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, status

from adapters.keywords_postgres import PostgresKeywordSource
from adapters.reporting_postgres import PostgresReportingSource
from adapters.keywords_yaml import YamlKeywordSource
from api.schemas import KeywordSyncRequest, KeywordUpsertRequest
from core.keywords_service import list_keywords
from core.keywords_materialize import materialize_call_keywords
from core.keywords_sync import sync_keywords_to_postgres
from domain.config import KEYWORDS_CONFIG
from domain.keywords import KeywordDefinition

router = APIRouter(prefix="/keywords", tags=["keywords"])

_SAFE_ID = re.compile(r"^[\w\-]+$")
_SAFE_ID_PATTERN = r"^[\w\-]+$"


def _get_keyword_source():
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        return PostgresKeywordSource(dsn)
    return YamlKeywordSource(KEYWORDS_CONFIG)


def _get_yaml_keyword_source() -> YamlKeywordSource:
    return YamlKeywordSource(KEYWORDS_CONFIG)


def _get_writable_keyword_source() -> PostgresKeywordSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Keyword catalog is read-only without POSTGRES_DSN",
        )
    return PostgresKeywordSource(dsn)


def _get_postgres_reporting_source() -> PostgresReportingSource:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
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
    "/sync",
    summary="Sync YAML keywords to Postgres",
    description=(
        "Copies keyword definitions from YAML config into Postgres.\n\n"
        "**Default**: `prune_missing=false` (existing Postgres-only rows are kept).\n\n"
        "Use this before `POST /keywords/materialize` when report drill-down should use new definitions."
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
    yaml_source = _get_yaml_keyword_source()
    try:
        try:
            return sync_keywords_to_postgres(
                yaml_source=yaml_source,
                postgres_source=postgres_source,
                prune_missing=req.prune_missing,
            )
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
        "Required for report drill-down endpoints under `/reports/keywords/{keyword_id}/...`."
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
        return materialize_call_keywords(
            reporting_source=reporting_source,
            keyword_source=keyword_source,
            keyword_store=keyword_source,
        )
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
            example="delivery",
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
            example="delivery",
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
            example="delivery",
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
