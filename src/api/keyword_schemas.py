"""Response models for keyword catalog and keyword analytics endpoints (BE-03).

These models are attached as ``response_model`` on the routes in
``src/api/routes/keywords.py`` and the keyword sections of
``src/api/routes/reports.py``.  Field names and optionality match the actual
serialized payloads exactly — no JSON renames.  Shared fragments come from
:mod:`api.response_schemas` and :mod:`api.report_schemas`.

Payload shapes verified against:
- ``core/keywords_service.py`` (catalog list, dynamic keyword report)
- ``adapters/keywords_postgres.py`` (materialized keyword report, calls/trend/
  managers drill-downs)
- ``core/keywords_sync.py``, ``core/keywords_materialize.py``,
  ``core/keywords_refresh.py`` (sync/materialize/refresh results)
- ``routes/reports.py`` and ``routes/keywords.py`` (freshness + keyword-AI
  attachments, AI-error attachment on maintenance endpoints)

Design notes:

- ``top_intents`` / ``top_outcomes`` are JSON arrays of ``[label, count]``
  pairs (serialized from Python tuples), same convention as BE-02.
- The single-keyword detail endpoint returns the *same* per-keyword object
  shape as one entry of the aggregate report's ``keywords`` list, plus the
  freshness and keyword-AI attachments.
- Maintenance endpoints (refresh/sync/materialize) may carry an extra
  ``keyword_ai_analysis_error`` key when the post-mutation AI analysis fails;
  it is modeled explicitly so OpenAPI documents it.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from api.report_schemas import ReportFiltersEcho
from api.response_schemas import FreshnessMetadata, KeywordAiAnalysisCompact


# ---------------------------------------------------------------------------
# Shared keyword fragments
# ---------------------------------------------------------------------------


class KeywordDefinitionEntry(BaseModel):
    """One keyword definition as returned by the catalog endpoints.

    Used both inside ``KeywordsCatalogResponse.keywords`` and as the direct
    response of ``GET /keywords/{keyword_id}`` (before attachments).
    """

    keyword_id: str = Field(
        description="Stable keyword identifier.", examples=["delivery"]
    )
    label: str = Field(
        description="Human-readable keyword label.", examples=["Delivery"]
    )
    category: str = Field(
        description="Keyword category.", examples=["logistics"]
    )
    terms: list[str] = Field(
        default_factory=list,
        description="Phrases used to match the keyword.",
        examples=[["delivery", "order"]],
    )
    match_fields: list[str] = Field(
        default_factory=list,
        description="Analysis fields scanned for term matches.",
        examples=[["summary", "key_questions", "objections"]],
    )
    is_active: bool = Field(description="Whether the keyword is active.")


class KeywordAggregateEntry(BaseModel):
    """Per-keyword match statistics (one entry in a keywords report).

    The same shape is returned directly by
    ``GET /reports/keywords/{keyword_id}`` (plus attachments).
    """

    keyword_id: str = Field(description="Stable keyword identifier.")
    label: str = Field(description="Human-readable keyword label.")
    category: str = Field(description="Keyword category.")
    terms: list[str] = Field(
        default_factory=list, description="Phrases used to match the keyword."
    )
    match_fields: list[str] = Field(
        default_factory=list, description="Analysis fields scanned for matches."
    )
    matched_calls: int = Field(description="Calls where this keyword matched.")
    total_matches: int = Field(description="Total term-match occurrences.")
    matched_managers: int = Field(description="Distinct managers with matches.")
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 10 intents as [label, count] pairs."
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 5 outcomes as [label, count] pairs."
    )


# ---------------------------------------------------------------------------
# Keyword catalog (src/api/routes/keywords.py)
# ---------------------------------------------------------------------------


class KeywordsCatalogResponse(BaseModel):
    """GET /keywords response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    data_source: str = Field(
        description="Active keyword data source name: 'postgres', 'json', or 'yaml'.",
        examples=["postgres"],
    )
    total_keywords: int = Field(description="Number of keywords in the catalog.")
    keywords: list[KeywordDefinitionEntry] = Field(
        default_factory=list, description="All keyword definitions."
    )


class KeywordDetailResponse(KeywordDefinitionEntry):
    """GET /keywords/{keyword_id} response.

    A single keyword definition object (same shape as one catalog entry).
    """


# ---------------------------------------------------------------------------
# Keyword maintenance results (refresh / sync / materialize)
# ---------------------------------------------------------------------------


class KeywordSyncResult(BaseModel):
    """Result of syncing YAML keywords into Postgres."""

    synced: int = Field(description="Number of keyword definitions upserted.")
    deleted: int = Field(
        description="Number of Postgres-only keywords deleted (prune_missing only)."
    )
    prune_missing: bool = Field(
        description="Whether prune_missing was enabled for this run."
    )
    synced_keyword_ids: list[str] = Field(
        default_factory=list, description="Sorted IDs of upserted keywords."
    )
    deleted_keyword_ids: list[str] = Field(
        default_factory=list, description="IDs of deleted keywords."
    )


class KeywordMaterializeResult(BaseModel):
    """Result of materializing keyword-to-call matches."""

    processed_calls: int = Field(description="Call records scanned.")
    matched_calls: int = Field(description="Calls with at least one match.")
    stored_rows: int = Field(description="Materialized match rows written.")
    active_keywords: int = Field(description="Active keywords used for matching.")


class KeywordRefreshResult(BaseModel):
    """POST /keywords/refresh response.

    Combines the sync and materialize results of the full refresh flow.
    """

    sync: KeywordSyncResult = Field(description="YAML -> Postgres sync result.")
    materialize: KeywordMaterializeResult = Field(
        description="Keyword match materialization result."
    )
    keyword_ai_analysis_error: str | None = Field(
        default=None,
        description=(
            "Present (non-null) only when the post-refresh AI analysis run "
            "failed; the refresh itself succeeded."
        ),
    )


class KeywordSyncResponse(BaseModel):
    """POST /keywords/sync response."""

    synced: int = Field(description="Number of keyword definitions upserted.")
    deleted: int = Field(
        description="Number of Postgres-only keywords deleted (prune_missing only)."
    )
    prune_missing: bool = Field(
        description="Whether prune_missing was enabled for this run."
    )
    synced_keyword_ids: list[str] = Field(
        default_factory=list, description="Sorted IDs of upserted keywords."
    )
    deleted_keyword_ids: list[str] = Field(
        default_factory=list, description="IDs of deleted keywords."
    )
    keyword_ai_analysis_error: str | None = Field(
        default=None,
        description=(
            "Present (non-null) only when the post-sync AI analysis run "
            "failed; the sync itself succeeded."
        ),
    )


class KeywordMaterializeResponse(BaseModel):
    """POST /keywords/materialize response."""

    processed_calls: int = Field(description="Call records scanned.")
    matched_calls: int = Field(description="Calls with at least one match.")
    stored_rows: int = Field(description="Materialized match rows written.")
    active_keywords: int = Field(description="Active keywords used for matching.")
    keyword_ai_analysis_error: str | None = Field(
        default=None,
        description=(
            "Present (non-null) only when the post-materialize AI analysis run "
            "failed; the materialization itself succeeded."
        ),
    )


# ---------------------------------------------------------------------------
# Keyword analytics reports (src/api/routes/reports.py)
# ---------------------------------------------------------------------------


class KeywordsReport(BaseModel):
    """GET /reports/keywords response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    report_data_source: str = Field(
        description=(
            "Source of the call data: 'postgres', 'json', 'yaml', or "
            "'postgres_materialized'."
        ),
        examples=["postgres_materialized"],
    )
    keyword_data_source: str = Field(
        description="Source of the keyword definitions.", examples=["postgres"]
    )
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    total_keywords: int = Field(description="Number of keywords in the report.")
    keywords_with_matches: int = Field(
        description="Keywords with at least one matched call."
    )
    keywords: list[KeywordAggregateEntry] = Field(
        default_factory=list, description="Per-keyword aggregates (sorted)."
    )
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None, description="Latest compact AI catalog analysis, or null."
    )


class KeywordDetailReport(KeywordAggregateEntry):
    """GET /reports/keywords/{keyword_id} response.

    A single per-keyword aggregate (same shape as one entry of the aggregate
    report) with freshness and keyword-AI attachments.
    """

    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None,
        description=(
            "Compact AI catalog analysis filtered to groups matching this "
            "keyword, or null when absent."
        ),
    )


class KeywordCallEntry(BaseModel):
    """Single matched call in the paginated keyword-calls report."""

    call_id: str = Field(description="Unique call identifier.")
    match_count: int = Field(description="Number of term matches for this call.")
    matched_fields: list[str] = Field(
        default_factory=list, description="Analysis fields where terms matched."
    )
    matched_terms: list[str] = Field(
        default_factory=list, description="Terms that matched."
    )
    call_datetime: str = Field(
        description="Call date (YYYY-MM-DD) or empty string when unknown."
    )
    manager_id: str = Field(description="Manager who handled the call.")
    manager_name: str = Field(description="Manager display name.")
    role: str = Field(description="Manager role.")
    direction: str = Field(description="Call direction.", examples=["incoming"])
    intent: str = Field(description="Call intent label from analysis.")
    outcome: str = Field(description="Call outcome label from analysis.")
    summary: str = Field(description="AI-generated call summary.")
    audio_seconds: float = Field(description="Call duration in seconds.")
    spam_probability: float = Field(description="Spam probability score (0..1).")
    effective_call: bool = Field(description="Whether the call was marked effective.")


class KeywordCallsReport(BaseModel):
    """GET /reports/keywords/{keyword_id}/calls response.

    The only true offset-paginated report endpoint: ``total_calls`` is the
    full match count, ``limit``/``offset`` echo the requested page.
    """

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    report_data_source: str = Field(
        description="Source of the call data.", examples=["postgres_materialized"]
    )
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    keyword_id: str = Field(description="Keyword the calls matched.")
    total_calls: int = Field(
        description="Total matching calls (before limit/offset)."
    )
    limit: int = Field(description="Page size requested.", examples=[50])
    offset: int = Field(description="Zero-based offset requested.", examples=[0])
    calls: list[KeywordCallEntry] = Field(
        default_factory=list, description="Matched calls for this page."
    )
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None,
        description=(
            "Compact AI catalog analysis filtered to groups matching this "
            "keyword, or null when absent."
        ),
    )


class KeywordTrendPoint(BaseModel):
    """Single date point in the keyword trend series."""

    call_datetime: str = Field(
        description="Date (YYYY-MM-DD) or empty string when unknown."
    )
    matched_calls: int = Field(description="Matched calls on this date.")
    total_matches: int = Field(description="Total term matches on this date.")


class KeywordTrendReport(BaseModel):
    """GET /reports/keywords/{keyword_id}/trend response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    report_data_source: str = Field(
        description="Source of the call data.", examples=["postgres_materialized"]
    )
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    keyword_id: str = Field(description="Keyword the trend is for.")
    points: list[KeywordTrendPoint] = Field(
        default_factory=list, description="Date-based trend series (ascending)."
    )
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None,
        description=(
            "Compact AI catalog analysis filtered to groups matching this "
            "keyword, or null when absent."
        ),
    )


class KeywordManagerEntry(BaseModel):
    """Per-manager breakdown for one keyword."""

    manager_id: str = Field(description="Stable manager identifier.")
    manager_name: str = Field(description="Manager display name.")
    role: str = Field(description="Manager role.")
    matched_calls: int = Field(description="Matched calls handled by this manager.")
    total_matches: int = Field(description="Total term matches for this manager.")


class KeywordManagersReport(BaseModel):
    """GET /reports/keywords/{keyword_id}/managers response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    report_data_source: str = Field(
        description="Source of the call data.", examples=["postgres_materialized"]
    )
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    keyword_id: str = Field(description="Keyword the breakdown is for.")
    managers: list[KeywordManagerEntry] = Field(
        default_factory=list, description="Manager-level aggregates (sorted)."
    )
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None,
        description=(
            "Compact AI catalog analysis filtered to groups matching this "
            "keyword, or null when absent."
        ),
    )
