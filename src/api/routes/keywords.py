import os
import re

from fastapi import APIRouter, HTTPException, status

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


@router.get("")
def keywords_catalog():
    source = _get_keyword_source()
    try:
        return list_keywords(source)
    finally:
        source.close()


@router.post("/sync")
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


@router.post("/materialize")
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


@router.get("/{keyword_id}")
def keyword_detail(keyword_id: str):
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


@router.post("", status_code=status.HTTP_201_CREATED)
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


@router.put("/{keyword_id}")
def update_keyword(keyword_id: str, req: KeywordUpsertRequest):
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


@router.delete("/{keyword_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_keyword(keyword_id: str):
    if not _SAFE_ID.match(keyword_id):
        raise HTTPException(status_code=400, detail="Invalid keyword_id")
    source = _get_writable_keyword_source()
    try:
        deleted = source.delete_keyword(keyword_id)
    finally:
        source.close()
    if not deleted:
        raise HTTPException(status_code=404, detail="Keyword not found")
