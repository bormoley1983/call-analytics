"""Response models for health, managers, and core report endpoints (BE-02).

These models are attached as ``response_model`` on the routes in
``src/api/routes/health.py``, ``managers.py``, and ``reports.py``.  Field
names and optionality match the actual serialized payloads exactly — no JSON
renames.  Shared fragments come from :mod:`api.response_schemas`.

Payload shapes verified against:
- ``core/reporting_service.py`` (JSON/YAML mode builders)
- ``adapters/reporting_postgres.py`` (Postgres SQL builders)
- ``routes/reports.py`` (freshness + keyword-AI attachments)
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from api.response_schemas import FreshnessMetadata, KeywordAiAnalysisCompact


# ---------------------------------------------------------------------------
# Shared report fragments
# ---------------------------------------------------------------------------


class ReportFiltersEcho(BaseModel):
    """Echo of the applied filters (``ReportFilters.as_dict()``).

    Dates are ISO strings or null; all fields are always present.
    """

    date_from: str | None = Field(
        default=None,
        description="Inclusive start date (YYYY-MM-DD) or null.",
    )
    date_to: str | None = Field(
        default=None,
        description="Inclusive end date (YYYY-MM-DD) or null.",
    )
    manager_id: str | None = Field(default=None)
    role: str | None = Field(default=None)
    direction: str | None = Field(default=None)
    intent: str | None = Field(default=None)
    outcome: str | None = Field(default=None)
    spam_only: bool = Field(default=False)
    effective_only: bool = Field(default=False)


class TopCountedItems(BaseModel):
    """Placeholder for top-N (label, count) pairs.

    Serialized as a JSON array of ``[label, count]`` pairs (Python tuples).
    Modeled as ``list[tuple[str, int]]`` on the parent models; this class is
    kept for OpenAPI documentation purposes only.
    """


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    """GET /health response."""

    status: str = Field(
        description="Always 'ok' when the API process is alive.",
        examples=["ok"],
    )
    ollama: str = Field(
        description="Ollama probe result: 'up' or 'down'.",
        examples=["up"],
    )
    ollama_url: str = Field(
        description="Configured Ollama base URL.",
        examples=["http://host.docker.internal:11434"],
    )


# ---------------------------------------------------------------------------
# Managers (config)
# ---------------------------------------------------------------------------


class ManagerEntry(BaseModel):
    """Single configured manager from GET /managers."""

    id: str = Field(description="Stable manager identifier.", examples=["petrenko_aa"])
    name: str = Field(description="Human-readable name.", examples=["Anna Petrenko"])
    role: str = Field(
        description="Manager role: 'management' or 'sales'.",
        examples=["sales"],
    )
    internal_extensions: list[str] = Field(
        default_factory=list,
        description="Internal PBX extensions (stringified).",
        examples=[["101"]],
    )
    external_lines: list[Any] = Field(
        default_factory=list,
        description="External line identifiers.",
        examples=[["+380671234567"]],
    )


# ---------------------------------------------------------------------------
# Overall report
# ---------------------------------------------------------------------------


class OverallReport(BaseModel):
    """GET /reports/overall response.

    ``top_intents`` / ``top_outcomes`` / ``top_questions`` are JSON arrays of
    ``[label, count]`` pairs (serialized from Python tuples).
    """

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    data_source: str = Field(
        description="Active data source name: 'postgres', 'json', or 'yaml'.",
        examples=["postgres"],
    )
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    total_calls: int = Field(description="Total calls matching filters.")
    analyzed_calls: int = Field(description="Calls with completed analysis.")
    unique_managers: int = Field(description="Distinct managers in the result set.")
    skipped_metrics_available: bool = Field(
        default=False,
        description="Whether skipped-call metrics are available for this source.",
    )
    spam_calls: int = Field(description="Calls at/above spam probability threshold.")
    effective_calls: int = Field(description="Calls marked effective.")
    total_duration_seconds: float = Field(description="Sum of call durations.")
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list,
        description="Top 10 intents as [label, count] pairs.",
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list,
        description="Top 5 outcomes as [label, count] pairs.",
    )
    top_questions: list[tuple[str, int]] = Field(
        default_factory=list,
        description="Top 10 questions as [label, count] pairs.",
    )
    freshness: FreshnessMetadata | None = Field(
        default=None,
        description="Present only in Postgres mode.",
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None,
        description="Latest compact AI catalog analysis, or null when absent.",
    )


# ---------------------------------------------------------------------------
# Managers report
# ---------------------------------------------------------------------------


class ManagerStats(BaseModel):
    """Per-manager aggregate (one entry in all_managers / by_role)."""

    manager_id: str = Field(description="Stable manager identifier.")
    manager_name: str = Field(description="Human-readable manager name.")
    role: str = Field(description="Manager role.", examples=["sales"])
    total_calls: int = 0
    incoming: int = 0
    outgoing: int = 0
    spam_calls: int = 0
    effective_calls: int = 0
    total_duration_seconds: float = 0.0
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 10 intents as [label, count] pairs."
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 5 outcomes as [label, count] pairs."
    )
    top_questions: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 10 questions as [label, count] pairs."
    )


class RoleSummaryEntry(BaseModel):
    """Per-role call count in role_summary."""

    total_calls: int = Field(description="Total calls for this role.")


class ManagersReport(BaseModel):
    """GET /reports/managers response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    data_source: str = Field(description="Active data source name.")
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    skipped_metrics_available: bool = Field(default=False)
    role_summary: dict[str, RoleSummaryEntry] = Field(
        default_factory=dict,
        description="Role → {total_calls} map.",
    )
    by_role: dict[str, list[ManagerStats]] = Field(
        default_factory=dict,
        description="Role → list of manager aggregates.",
    )
    all_managers: list[ManagerStats] = Field(
        default_factory=list,
        description="All manager aggregates (sorted).",
    )
    total_managers: int = Field(description="Number of managers in the result set.")
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None, description="Latest compact AI catalog analysis, or null."
    )


# ---------------------------------------------------------------------------
# Customers report
# ---------------------------------------------------------------------------


class CustomerManagerRef(BaseModel):
    """Manager reference inside a customer aggregate."""

    manager_id: str = Field(description="Stable manager identifier.")
    manager_name: str = Field(description="Human-readable manager name.")
    role: str = Field(description="Manager role.")
    calls: int = Field(description="Call count for this manager.")


class CustomerStats(BaseModel):
    """Per-customer aggregate (one entry in all_customers)."""

    customer_phone: str = Field(
        description="Normalized customer phone (digits only).",
        examples=["380671234567"],
    )
    display_phone: str = Field(
        description="Human-readable phone as seen in call records.",
        examples=["+38 (067) 123-45-67"],
    )
    total_calls: int = 0
    incoming: int = 0
    outgoing: int = 0
    spam_calls: int = 0
    effective_calls: int = 0
    total_duration_seconds: float = 0.0
    first_call_date: str | None = Field(
        default=None, description="Earliest call date (YYYY-MM-DD) or null."
    )
    last_call_date: str | None = Field(
        default=None, description="Latest call date (YYYY-MM-DD) or null."
    )
    top_intents: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 10 intents as [label, count] pairs."
    )
    top_outcomes: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 5 outcomes as [label, count] pairs."
    )
    top_questions: list[tuple[str, int]] = Field(
        default_factory=list, description="Top 10 questions as [label, count] pairs."
    )
    managers: list[CustomerManagerRef] = Field(
        default_factory=list,
        description="Managers who called this customer (sorted by call count).",
    )


class CustomersReport(BaseModel):
    """GET /reports/customers response."""

    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    data_source: str = Field(description="Active data source name.")
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    skipped_metrics_available: bool = Field(default=False)
    all_customers: list[CustomerStats] = Field(
        default_factory=list, description="All customer aggregates (sorted)."
    )
    total_customers: int = Field(description="Number of customers in the result set.")
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None, description="Latest compact AI catalog analysis, or null."
    )


# ---------------------------------------------------------------------------
# Customer follow-up detail
# ---------------------------------------------------------------------------


class CustomerCallEntry(BaseModel):
    """Single call in the customer follow-up history."""

    call_id: str = Field(description="Unique call identifier.")
    call_date: str = Field(
        description="Call date (YYYY-MM-DD) or empty string when unknown.",
    )
    direction: str = Field(description="Call direction.", examples=["incoming"])
    manager_id: str = Field(description="Manager who handled the call.")
    manager_name: str = Field(description="Manager display name.")
    role: str = Field(description="Manager role.")
    spam_probability: float = Field(description="Spam probability score (0..1).")
    effective_call: bool = Field(description="Whether the call was marked effective.")
    intent: str = Field(description="Call intent label from analysis.")
    outcome: str = Field(description="Call outcome label from analysis.")
    summary: str = Field(description="AI-generated call summary.")
    audio_seconds: float = Field(description="Call duration in seconds.")
    src_number: str = Field(description="Source phone number (raw).")
    dst_number: str = Field(description="Destination phone number (raw).")
    key_questions: list[str] = Field(
        default_factory=list, description="Key questions raised during the call."
    )
    objections: list[str] = Field(
        default_factory=list, description="Objections raised during the call."
    )


class CustomerFollowupReport(CustomerStats):
    """GET /reports/customers/{customer_phone} response.

    Extends the customer aggregate with the full call history and report
    metadata.
    """

    calls: list[CustomerCallEntry] = Field(
        default_factory=list,
        description="Full follow-up history (newest first).",
    )
    generated_at: str = Field(description="ISO 8601 UTC generation timestamp.")
    data_source: str = Field(description="Active data source name.")
    filters: ReportFiltersEcho = Field(description="Applied filter echo.")
    skipped_metrics_available: bool = Field(default=False)
    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None, description="Latest compact AI catalog analysis, or null."
    )


# ---------------------------------------------------------------------------
# Single manager report (GET /reports/manager/{manager_id})
# ---------------------------------------------------------------------------


class ManagerReportDetail(ManagerStats):
    """GET /reports/manager/{manager_id} response.

    A single manager aggregate with freshness and keyword-AI attachments.
    """

    freshness: FreshnessMetadata | None = Field(
        default=None, description="Present only in Postgres mode."
    )
    keyword_ai_analysis: KeywordAiAnalysisCompact | None = Field(
        default=None, description="Latest compact AI catalog analysis, or null."
    )
