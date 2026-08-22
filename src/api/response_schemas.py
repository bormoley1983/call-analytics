"""Shared response-schema building blocks for the public API.

These models describe the *shape* of JSON payloads already produced by the
routes.  They are intentionally **not** attached as ``response_model`` yet —
that happens in BE-02..BE-04.  This module only provides reusable, explicitly
typed components so that later tasks can compose endpoint response models
without re-inventing freshness, pagination, error, or identifier fragments.

Design rules (BE-01):

- Field names and optionality must match the actual serialized payloads
  exactly.  No JSON renames in this task.
- ``Optional[T]`` with ``default=None`` means "key may be absent *or* null".
  Fields that are always present but sometimes null use ``T | None`` without
  a default (required, nullable).
- Domain models must not leak into the API layer; these are pure Pydantic
  value objects.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Error / validation building blocks
# ---------------------------------------------------------------------------


class ApiErrorDetail(BaseModel):
    """Normalized single error detail entry.

    Mirrors the shape FastAPI puts inside ``{"detail": [...]}``` for 422
    responses and ``{"detail": "..."}`` for other HTTP errors.  The frontend
    can rely on this stable structure for user-facing messages.
    """

    loc: list[str | int] = Field(
        default_factory=list,
        description="Path to the offending field (empty for body-level errors).",
        examples=[["body", "actions", 0, "keyword_id"]],
    )
    msg: str = Field(
        description="Human-readable error message.",
        examples=["Value error, AIApplyAction requires either (group_index + action_index) or keyword_id"],
    )
    type: str = Field(
        description="Stable error type identifier for frontend logic.",
        examples=["value_error", "missing", "int_parsing"],
    )


class ApiValidationError(BaseModel):
    """422 validation-error envelope.

    Matches FastAPI's default ``{"detail": [{loc, msg, type}, ...]}`` shape.
    The frontend should key off ``type`` for programmatic handling and show
    ``msg`` to the user.
    """

    detail: list[ApiErrorDetail] = Field(
        description="List of validation errors.",
    )


class ApiError(BaseModel):
    """Generic non-422 error envelope.

    Matches FastAPI's default ``{"detail": "..."}`` shape used by 400/404/405/
    409/500/502 responses.  The frontend can branch on the HTTP status code and
    display ``detail`` as-is.
    """

    detail: str = Field(
        description="Human-readable error message.",
        examples=["Keyword matches are not materialized yet"],
    )


# ---------------------------------------------------------------------------
# Freshness metadata
# ---------------------------------------------------------------------------


class FreshnessMetadata(BaseModel):
    """Data-freshness timestamps attached to report payloads.

    Present only when ``POSTGRES_DSN`` is configured (Postgres mode).  In
    JSON/YAML dev mode the key is absent from the response.
    """

    latest_processed_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp of the most recently analyzed call.",
    )
    latest_materialized_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp of the last keyword materialization run.",
    )
    latest_keyword_ai_analysis_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp of the most recent AI catalog analysis.",
    )
    keyword_ai_analysis_status: str = Field(
        description=(
            "Derived status: 'available' (analysis is up-to-date), "
            "'missing' (no analysis yet), or 'stale' (analysis older than "
            "latest processed/materialized data)."
        ),
        examples=["available"],
    )


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class KeywordCallsPagination(BaseModel):
    """Offset/limit pagination fields embedded in the keyword-calls report.

    The paginated endpoint ``GET /reports/keywords/{keyword_id}/calls`` returns
    a flat dict (not a nested envelope) with these keys alongside ``calls``,
    ``total_calls``, ``filters``, etc.  Verified against
    ``PostgresKeywordSource.build_keyword_calls_report``.
    """

    total_calls: int = Field(
        description="Total number of matching calls (before limit/offset).",
    )
    limit: int = Field(
        description="Page size requested.",
        examples=[50],
    )
    offset: int = Field(
        description="Zero-based offset requested.",
        examples=[0],
    )


# ---------------------------------------------------------------------------
# Compact keyword-AI analysis fragments
# ---------------------------------------------------------------------------


class KeywordAiGroupAction(BaseModel):
    """Single suggested action within a keyword-AI group.

    The route serializes each action as an opaque dict; this model types the
    fields that are consistently present.  Extra keys are preserved via
    ``model_config`` in the parent if needed.
    """

    action_type: str | None = Field(
        default=None,
        description="Type of suggested action (merge, rename, deactivate, etc.).",
    )
    target_keyword_id: str | None = Field(
        default=None,
        description="Target keyword for merge/rename actions.",
    )
    suggested_label: str | None = Field(
        default=None,
        description="Suggested label for rename actions.",
    )


class KeywordAiGroup(BaseModel):
    """Compact representation of one semantic group from the AI catalog analysis.

    Produced by ``_compact_keyword_ai_group`` in ``reports.py``.  At most 5
    groups are returned for aggregate reports; keyword-scoped reports return
    only matching groups.
    """

    group_label: str | None = Field(
        default=None,
        description="Short label for the group.",
    )
    theme: str | None = Field(
        default=None,
        description="Semantic theme of the group.",
    )
    keywords: list[str] = Field(
        default_factory=list,
        description="Keyword IDs belonging to this group.",
    )
    primary_keyword_id: str | None = Field(
        default=None,
        description="Primary keyword ID for the group (used for matching).",
    )
    suggested_category: str | None = Field(
        default=None,
        description="Suggested category for merged/renamed keywords.",
    )
    suggested_shared_terms: list[str] = Field(
        default_factory=list,
        description="Terms shared across the group's keywords.",
    )
    suggested_actions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Suggested actions (kept as dicts; typed in BE-04 if needed).",
    )
    rationale: str | None = Field(
        default=None,
        description="AI explanation for the grouping.",
    )


class KeywordAiAnalysisCompact(BaseModel):
    """Compact keyword-AI analysis payload attached to report responses.

    Present as ``keyword_ai_analysis`` on every report endpoint.  The value is
    ``null`` (key present, value null) when no AI analysis exists or the data
    source is JSON/YAML mode.
    """

    analysis_id: str | None = Field(
        default=None,
        description="ID of the latest AI catalog analysis.",
    )
    created_at: str | None = Field(
        default=None,
        description="ISO 8601 creation timestamp of the analysis.",
    )
    summary: str | None = Field(
        default=None,
        description="AI-generated summary of the catalog state.",
    )
    global_recommendations: list[str] = Field(
        default_factory=list,
        description="Global recommendations from the AI analysis.",
    )
    groups_total: int = Field(
        default=0,
        description="Total number of groups in the full analysis.",
    )
    groups_returned: int = Field(
        default=0,
        description="Number of groups included in this compact payload.",
    )
    groups: list[KeywordAiGroup] = Field(
        default_factory=list,
        description="Compact group representations (max 5 for aggregate reports).",
    )


# ---------------------------------------------------------------------------
# Reusable identifiers
# ---------------------------------------------------------------------------


class ManagerIdentifier(BaseModel):
    """Minimal manager identity used across reports and config."""

    id: str = Field(
        description="Stable manager identifier.",
        examples=["petrenko_aa"],
    )
    name: str = Field(
        description="Human-readable manager name.",
        examples=["Anna Petrenko"],
    )


class KeywordIdentifier(BaseModel):
    """Minimal keyword identity used across catalog and reports."""

    keyword_id: str = Field(
        description="Stable keyword identifier.",
        examples=["delivery"],
    )
    label: str = Field(
        description="Human-readable keyword label.",
        examples=["Delivery"],
    )


# ---------------------------------------------------------------------------
# Collection envelope (shared wrapper for list responses)
# ---------------------------------------------------------------------------


class CollectionEnvelope(BaseModel):
    """Generic envelope for list/collection responses.

    Many endpoints return ``{"items": [...], ...}`` or a bare list.  This model
    is provided for BE-02..BE-04 to use when the response wraps items in an
    object with metadata (e.g., ``{"returned": N, "analyses": [...]}``).
    Endpoints returning bare lists do not need this wrapper.

    The concrete item type varies per endpoint; compose with a subclass or
    add an ``items``/named-list field in the endpoint-specific model.
    """

    returned: int = Field(
        description="Number of items in the collection.",
    )
