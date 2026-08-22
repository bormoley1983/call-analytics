"""Response schemas for keyword generation and AI workflow endpoints (BE-04).

These models describe the *shape* of JSON payloads produced by the routes in
``keywords_generation.py`` and ``keywords_ai.py``.  They are attached as
``response_model`` on those routes so that OpenAPI documents concrete success
types for every AI Review UI endpoint.

Design rules (BE-04):

- Field names and optionality must match the actual serialized payloads
  exactly.  No JSON renames.
- Partial/skipped/apply results are modeled with explicit fields, never
  collapsed into a single success/failure flag.
- Dry-run vs applied is distinguishable via the ``dry_run`` field on
  ``AIApplyResult`` (already present in ``api.schemas``).
- Reuses existing typed models from ``api.schemas`` where adequate
  (``KeywordGenerationEnrichResult``, ``DeepInsightResult``,
  ``DeepInsightRunEntry``, ``KeywordAliasExpandResult``, etc.).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Generation — candidate generation
# ---------------------------------------------------------------------------


class KeywordGenerationCandidate(BaseModel):
    """Single generated keyword candidate."""

    candidate_id: str = Field(
        description="Stable identifier derived from the phrase.",
        examples=["cand_delivery_delay"],
    )
    phrase: str = Field(
        description="The extracted candidate phrase.",
        examples=["delivery delay"],
    )
    support_calls: int = Field(
        description="Number of unique calls containing this phrase.",
    )
    total_matches: int = Field(
        description="Total number of phrase matches across all scanned texts.",
    )
    sample_call_ids: list[str] = Field(
        default_factory=list,
        description="Sample call IDs where this phrase was found (max 5).",
    )
    suggested_keyword_id: str = Field(
        description="Auto-generated keyword_id for this candidate.",
        examples=["delivery_delay"],
    )
    suggested_label: str = Field(
        description="Auto-derived human-readable label.",
        examples=["Delivery Delay"],
    )
    suggested_match_fields: list[str] = Field(
        default_factory=list,
        description="Match fields selected for this candidate.",
    )


class KeywordGenerationCandidatesResponse(BaseModel):
    """Response for POST /keywords/generation/candidates."""

    processed_calls: int = Field(
        description="Number of call records scanned.",
    )
    candidate_count: int = Field(
        description="Number of candidates returned (after max_candidates cap).",
    )
    excluded_existing_terms: bool = Field(
        description="Whether existing keyword terms were excluded from candidates.",
    )
    skipped_existing_term_hits: int = Field(
        description="Number of phrase hits skipped because the term already exists.",
    )
    selected_match_fields: list[str] = Field(
        default_factory=list,
        description="Match fields used for candidate extraction.",
    )
    candidates: list[KeywordGenerationCandidate] = Field(
        default_factory=list,
        description="Ranked candidate phrases.",
    )
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Echo of the ReportFilters used for this generation run.",
    )


# ---------------------------------------------------------------------------
# Generation — publish
# ---------------------------------------------------------------------------


class KeywordPublishResult(BaseModel):
    """Result of publishing generated candidates into the keyword catalog."""

    created: int = Field(description="Number of new keywords created.")
    updated: int = Field(description="Number of existing keywords updated.")
    skipped_existing_terms: int = Field(
        description="Number of candidates skipped because the term already exists.",
    )
    skipped_invalid: int = Field(
        description="Number of candidates skipped due to invalid/empty phrase.",
    )
    created_keyword_ids: list[str] = Field(
        default_factory=list,
        description="Keyword IDs that were newly created.",
    )
    updated_keyword_ids: list[str] = Field(
        default_factory=list,
        description="Keyword IDs that were updated with new terms.",
    )
    skipped_existing_term_values: list[str] = Field(
        default_factory=list,
        description="Phrases that were skipped because they already exist.",
    )


class KeywordPublishResponse(BaseModel):
    """Response for POST /keywords/generation/publish."""

    publish: KeywordPublishResult = Field(
        description="Publish result with created/updated/skipped counts.",
    )
    materialized: bool = Field(
        description="Whether keyword materialization was triggered after publish.",
    )
    materialize: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Materialization result when ``materialized`` is true. "
            "Absent or null otherwise."
        ),
    )


# ---------------------------------------------------------------------------
# Generation — bootstrap / pipeline (shared shape)
# ---------------------------------------------------------------------------


class KeywordGenerationPipelineResponse(BaseModel):
    """Response for POST /keywords/generation/bootstrap and /pipeline.

    Models the full generate → (enrich) → publish → (materialize) →
    (AI analyze) flow without collapsing partial results into a single
    success/failure flag.  Each stage is independently inspectable.
    """

    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Echo of the ReportFilters used for this pipeline run.",
    )
    generation: KeywordGenerationCandidatesResponse = Field(
        description="Candidate generation result (without the filters echo).",
    )
    enrichment: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Enrichment result when ``enrich_before_publish`` was true and "
            "candidates were available.  Null otherwise."
        ),
    )
    publish: KeywordPublishResult = Field(
        description="Publish result with created/updated/skipped counts.",
    )
    materialized: bool = Field(
        description="Whether keyword materialization was triggered after publish.",
    )
    keyword_ai_analysis: dict[str, Any] | None = Field(
        default=None,
        description=(
            "AI catalog analysis result when ``run_ai_analysis_after_publish`` "
            "was true and changes were made.  Null otherwise."
        ),
    )
    materialize: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Materialization result when ``materialized`` is true. "
            "Absent or null otherwise."
        ),
    )


# ---------------------------------------------------------------------------
# AI catalog analysis — run
# ---------------------------------------------------------------------------


class KeywordAnalysisKeywordEntry(BaseModel):
    """Single keyword in the AI analysis input."""

    keyword_id: str = Field(description="Stable keyword identifier.")
    label: str = Field(description="Human-readable label.")
    category: str = Field(description="Keyword category.")
    terms: list[str] = Field(default_factory=list, description="Match terms.")
    match_fields: list[str] = Field(
        default_factory=list, description="Fields this keyword matches against."
    )
    is_active: bool = Field(description="Whether the keyword is active.")
    matched_calls: int = Field(
        default=0, description="Number of calls matching this keyword."
    )
    total_matches: int = Field(
        default=0, description="Total term matches across all calls."
    )
    matched_managers: int = Field(
        default=0, description="Number of distinct managers with matched calls."
    )
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list,
        description="Top intents as (label, count) pairs.",
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list,
        description="Top outcomes as (label, count) pairs.",
    )


class KeywordAnalysisCustomerContext(BaseModel):
    """Single customer in the AI analysis context."""

    customer_phone: str = Field(description="Normalized phone number.")
    display_phone: str = Field(description="Formatted phone for display.")
    total_calls: int = Field(description="Total calls from this customer.")
    effective_calls: int = Field(description="Effective (non-spam) calls.")
    spam_calls: int = Field(description="Spam calls.")
    last_call_date: str | None = Field(
        default=None, description="ISO date of the most recent call."
    )
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list, description="Top intents as (label, count) pairs."
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list, description="Top outcomes as (label, count) pairs."
    )
    top_questions: list[str] = Field(
        default_factory=list, description="Most frequent key questions."
    )
    managers: list[str] = Field(
        default_factory=list, description="Manager IDs associated with this customer."
    )


class KeywordAiAnalysisGroup(BaseModel):
    """Single semantic group from the AI catalog analysis."""

    group_label: str | None = Field(
        default=None, description="Short label for the group."
    )
    theme: str | None = Field(default=None, description="Semantic theme.")
    keywords: list[str] = Field(
        default_factory=list, description="Keyword IDs in this group."
    )
    primary_keyword_id: str | None = Field(
        default=None, description="Primary keyword ID for the group."
    )
    suggested_category: str | None = Field(
        default=None, description="Suggested category for merged keywords."
    )
    suggested_shared_terms: list[str] = Field(
        default_factory=list, description="Terms shared across the group."
    )
    suggested_actions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Suggested actions (merge, rename, deactivate, etc.).",
    )
    rationale: str | None = Field(
        default=None, description="AI explanation for the grouping."
    )


class KeywordAiAnalysisPayload(BaseModel):
    """The ``ai_analysis`` sub-payload from the LLM."""

    summary: str = Field(
        description="AI-generated summary of the catalog state.",
    )
    groups: list[KeywordAiAnalysisGroup] = Field(
        default_factory=list,
        description="Semantic groups suggested by the AI.",
    )
    ungrouped_keyword_ids: list[str] = Field(
        default_factory=list,
        description="Keyword IDs not assigned to any group.",
    )
    global_recommendations: list[str] = Field(
        default_factory=list,
        description="Global recommendations from the AI analysis.",
    )


class KeywordCatalogAnalysisResponse(BaseModel):
    """Response for POST /keywords/catalog/analysis.

    Combines the analysis input (what was sent to the LLM) with the AI
    result and optional persistence history.  The UI can distinguish:
    - **advice**: ``ai_analysis`` contains groups/recommendations (no mutation).
    - **history**: ``analysis_history`` is non-null when persisted.
    """

    keyword_source: str = Field(
        description="Name of the keyword data source used.",
    )
    reporting_source: str | None = Field(
        default=None,
        description="Name of the reporting source (null when match stats disabled).",
    )
    analyzed_keywords: int = Field(
        description="Number of keywords included in this analysis run.",
    )
    total_candidates_before_limit: int = Field(
        description="Total candidate keywords before max_keywords truncation.",
    )
    truncated: bool = Field(
        description="Whether the keyword list was truncated by max_keywords.",
    )
    keywords: list[KeywordAnalysisKeywordEntry] = Field(
        default_factory=list,
        description="Keywords included in the analysis input.",
    )
    customer_context: list[KeywordAnalysisCustomerContext] = Field(
        default_factory=list,
        description="Top customers providing context for the analysis.",
    )
    ai_analysis: KeywordAiAnalysisPayload = Field(
        description="AI-generated grouping and recommendations (advisory only).",
    )
    analysis_history: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Persistence metadata when POSTGRES_DSN is configured. "
            "Contains ``analysis_id`` and ``stored_items``.  Null in JSON mode."
        ),
    )


# ---------------------------------------------------------------------------
# AI catalog analysis — list / detail (persisted history)
# ---------------------------------------------------------------------------


class KeywordAnalysisSummaryEntry(BaseModel):
    """Single entry from the persisted analyses list."""

    analysis_id: str = Field(description="UUID of the analysis run.")
    keyword_source: str | None = Field(
        default=None, description="Keyword source name at analysis time."
    )
    reporting_source: str | None = Field(
        default=None, description="Reporting source name at analysis time."
    )
    ai_model: str | None = Field(
        default=None, description="AI model used for this analysis."
    )
    ai_summary: str | None = Field(
        default=None, description="Truncated AI summary."
    )
    analyzed_keywords: int = Field(
        default=0, description="Number of keywords analyzed."
    )
    total_candidates_before_limit: int = Field(
        default=0, description="Total candidates before truncation."
    )
    truncated: bool = Field(default=False, description="Whether list was truncated.")
    created_at: str = Field(description="ISO 8601 creation timestamp.")


class KeywordAnalysesListResponse(BaseModel):
    """Response for GET /keywords/catalog/analyses."""

    returned: int = Field(description="Number of analyses in this response.")
    analyses: list[KeywordAnalysisSummaryEntry] = Field(
        default_factory=list,
        description="Persisted analysis runs (newest first).",
    )


class KeywordAnalysisDetailResponse(BaseModel):
    """Response for GET /keywords/catalog/analyses/{analysis_id}.

    Full persisted analysis including the original request, input payload,
    AI result, and stored items by type.
    """

    analysis_id: str = Field(description="UUID of the analysis run.")
    keyword_source: str | None = Field(
        default=None, description="Keyword source name."
    )
    reporting_source: str | None = Field(
        default=None, description="Reporting source name."
    )
    ai_model: str | None = Field(default=None, description="AI model used.")
    ai_summary: str | None = Field(default=None, description="AI summary text.")
    analyzed_keywords: int = Field(default=0, description="Keywords analyzed.")
    total_candidates_before_limit: int = Field(
        default=0, description="Total candidates before truncation."
    )
    truncated: bool = Field(default=False, description="Whether list was truncated.")
    request: dict[str, Any] = Field(
        default_factory=dict,
        description="Original request parameters for this analysis run.",
    )
    analysis_input: dict[str, Any] = Field(
        default_factory=dict,
        description="Full analysis input payload sent to the LLM.",
    )
    ai_analysis: KeywordAiAnalysisPayload = Field(
        description="AI-generated grouping and recommendations.",
    )
    created_at: str = Field(description="ISO 8601 creation timestamp.")
    items: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict,
        description=(
            "Stored analysis items keyed by item type (e.g. 'keywords', "
            "'groups', 'actions'). Each item has ``item_key``, ``data``, "
            "and ``created_at``."
        ),
    )


# ---------------------------------------------------------------------------
# Alias suggestions — approve / reject result
# ---------------------------------------------------------------------------


class AliasSuggestionActionResult(BaseModel):
    """Result of approving or rejecting an alias suggestion.

    The store returns a plain bool; the route wraps it so the UI gets a
    stable, self-describing response instead of a bare ``true``/``false``.
    """

    suggestion_id: str = Field(description="UUID of the suggestion acted upon.")
    approved: bool | None = Field(
        default=None,
        description=(
            "True when the action was approve and succeeded. "
            "Null for reject actions."
        ),
    )
    rejected: bool | None = Field(
        default=None,
        description=(
            "True when the action was reject and succeeded. "
            "Null for approve actions."
        ),
    )
    success: bool = Field(
        description="Whether the store operation succeeded (row existed).",
    )


# ---------------------------------------------------------------------------
# Deep insights — run detail
# ---------------------------------------------------------------------------


class DeepInsightRunDetail(BaseModel):
    """Response for GET /keywords/catalog/insights/deep/runs/{run_id}.

    Combines the run metadata with all insights generated in that run.
    """

    run: dict[str, Any] = Field(
        description=(
            "Run metadata: ``run_id``, ``ai_model``, ``insight_types``, "
            "``request_data``, ``created_at``."
        ),
    )
    insights: list[dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "All insights from this run. Each has ``insight_id``, "
            "``insight_type``, ``title``, ``description``, ``severity``, "
            "``affected_calls_count``, ``evidence_summary``, ``metadata``, "
            "``created_at``."
        ),
    )
