"""Serialization and schema tests for shared response building blocks (BE-01).

Verifies that the models in ``src/api/response_schemas.py``:
- generate meaningful OpenAPI schemas;
- round-trip the exact JSON shapes produced by the routes;
- handle nullable vs absent fields correctly.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, TypeAdapter

from api.response_schemas import (
    ApiError,
    ApiErrorDetail,
    ApiValidationError,
    CollectionEnvelope,
    FreshnessMetadata,
    KeywordAiAnalysisCompact,
    KeywordAiGroup,
    KeywordCallsPagination,
    KeywordIdentifier,
    ManagerIdentifier,
)


# ---------------------------------------------------------------------------
# OpenAPI schema generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model_cls",
    [
        ApiErrorDetail,
        ApiValidationError,
        ApiError,
        FreshnessMetadata,
        KeywordCallsPagination,
        KeywordAiGroup,
        KeywordAiAnalysisCompact,
        ManagerIdentifier,
        KeywordIdentifier,
        CollectionEnvelope,
    ],
)
def test_openapi_schema_has_properties(model_cls: type[BaseModel]) -> None:
    """Every shared model must produce a non-trivial OpenAPI schema."""
    schema = model_cls.model_json_schema()
    assert "properties" in schema, f"{model_cls.__name__} has no properties"
    assert len(schema["properties"]) > 0


def test_api_error_detail_openapi_types() -> None:
    schema = ApiErrorDetail.model_json_schema()
    props = schema["properties"]
    assert "loc" in props
    assert "msg" in props
    assert "type" in props
    # loc is a list of string-or-int
    assert props["loc"]["type"] == "array"


def test_freshness_metadata_openapi_nullable_fields() -> None:
    schema = FreshnessMetadata.model_json_schema()
    props = schema["properties"]
    for field_name in (
        "latest_processed_at",
        "latest_materialized_at",
        "latest_keyword_ai_analysis_at",
    ):
        assert field_name in props
        # nullable: anyOf with null type
        assert "anyOf" in props[field_name] or "nullable" in props[field_name]
    # status is required and non-nullable
    assert "keyword_ai_analysis_status" in schema.get("required", [])


# ---------------------------------------------------------------------------
# Serialization round-trips (match actual route payloads)
# ---------------------------------------------------------------------------


def test_api_error_serializes_fastapi_shape() -> None:
    """400/404/405/409/500/502 responses use {"detail": "string"}."""
    err = ApiError(detail="Keyword matches are not materialized yet")
    assert json.loads(err.model_dump_json()) == {
        "detail": "Keyword matches are not materialized yet"
    }


def test_api_validation_error_serializes_fastapi_shape() -> None:
    """422 responses use {"detail": [{loc, msg, type}, ...]}."""
    payload = {
        "detail": [
            {"loc": ["body", "actions", 0], "msg": "Field required", "type": "missing"},
            {"loc": ["query", "limit"], "msg": "Input should be a valid integer", "type": "int_parsing"},
        ]
    }
    err = ApiValidationError.model_validate(payload)
    assert json.loads(err.model_dump_json()) == payload


def test_api_error_detail_defaults() -> None:
    """Body-level errors have empty loc."""
    detail = ApiErrorDetail(msg="Something went wrong", type="internal_error")
    dumped = detail.model_dump()
    assert dumped["loc"] == []
    assert dumped["msg"] == "Something went wrong"
    assert dumped["type"] == "internal_error"


def test_freshness_metadata_full_payload() -> None:
    """Matches the dict produced by _get_freshness_metadata in Postgres mode."""
    payload = {
        "latest_processed_at": "2026-08-20T14:30:00",
        "latest_materialized_at": "2026-08-20T15:00:00",
        "latest_keyword_ai_analysis_at": "2026-08-19T10:00:00",
        "keyword_ai_analysis_status": "stale",
    }
    meta = FreshnessMetadata.model_validate(payload)
    assert json.loads(meta.model_dump_json()) == payload


def test_freshness_metadata_all_null_timestamps() -> None:
    """Fresh DB: all timestamps null, status 'missing'."""
    payload = {
        "latest_processed_at": None,
        "latest_materialized_at": None,
        "latest_keyword_ai_analysis_at": None,
        "keyword_ai_analysis_status": "missing",
    }
    meta = FreshnessMetadata.model_validate(payload)
    assert meta.latest_processed_at is None
    assert meta.keyword_ai_analysis_status == "missing"


def test_freshness_metadata_absent_timestamp_keys() -> None:
    """Keys may be absent entirely (default None)."""
    meta = FreshnessMetadata(keyword_ai_analysis_status="available")
    dumped = meta.model_dump()
    assert dumped["latest_processed_at"] is None
    assert dumped["latest_materialized_at"] is None
    assert dumped["latest_keyword_ai_analysis_at"] is None


def test_keyword_calls_pagination_matches_report_shape() -> None:
    """Matches the flat pagination keys in build_keyword_calls_report output."""
    payload = {"total_calls": 1234, "limit": 50, "offset": 100}
    page = KeywordCallsPagination.model_validate(payload)
    assert json.loads(page.model_dump_json()) == payload


def test_keyword_ai_group_compact_shape() -> None:
    """Matches _compact_keyword_ai_group output."""
    payload = {
        "group_label": "Delivery issues",
        "theme": "logistics",
        "keywords": ["delivery", "shipping"],
        "primary_keyword_id": "delivery",
        "suggested_category": "logistics",
        "suggested_shared_terms": ["delivery", "package"],
        "suggested_actions": [
            {"action_type": "merge", "target_keyword_id": "shipping"}
        ],
        "rationale": "Both keywords track delivery status.",
    }
    group = KeywordAiGroup.model_validate(payload)
    assert json.loads(group.model_dump_json()) == payload


def test_keyword_ai_group_minimal() -> None:
    """All optional fields absent → defaults."""
    group = KeywordAiGroup()
    dumped = group.model_dump()
    assert dumped["group_label"] is None
    assert dumped["keywords"] == []
    assert dumped["suggested_actions"] == []


def test_keyword_ai_analysis_compact_full() -> None:
    """Matches _build_keyword_ai_analysis_payload output."""
    payload = {
        "analysis_id": "abc-123",
        "created_at": "2026-08-19T10:00:00",
        "summary": "Catalog has 5 overlapping groups.",
        "global_recommendations": ["Merge delivery/shipping"],
        "groups_total": 8,
        "groups_returned": 5,
        "groups": [
            {
                "group_label": "Delivery issues",
                "theme": "logistics",
                "keywords": ["delivery"],
                "primary_keyword_id": "delivery",
                "suggested_category": None,
                "suggested_shared_terms": [],
                "suggested_actions": [],
                "rationale": None,
            }
        ],
    }
    compact = KeywordAiAnalysisCompact.model_validate(payload)
    assert json.loads(compact.model_dump_json()) == payload


def test_keyword_ai_analysis_compact_null_value() -> None:
    """When no analysis exists the route sets keyword_ai_analysis=None.

    The model itself is not instantiated in that case; this test documents
    that the field on the parent report model must be Optional with default
    None (key present, value null).
    """
    adapter: TypeAdapter[KeywordAiAnalysisCompact | None] = TypeAdapter(
        KeywordAiAnalysisCompact | None
    )
    assert adapter.validate_python(None) is None


def test_manager_identifier_roundtrip() -> None:
    payload = {"id": "petrenko_aa", "name": "Anna Petrenko"}
    ident = ManagerIdentifier.model_validate(payload)
    assert json.loads(ident.model_dump_json()) == payload


def test_keyword_identifier_roundtrip() -> None:
    payload = {"keyword_id": "delivery", "label": "Delivery"}
    ident = KeywordIdentifier.model_validate(payload)
    assert json.loads(ident.model_dump_json()) == payload


def test_collection_envelope_roundtrip() -> None:
    """Matches GET /keywords/catalog/analyses envelope shape."""
    env = CollectionEnvelope(returned=42)
    dumped = env.model_dump()
    assert dumped["returned"] == 42
