from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    done = "done"
    failed = "failed"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


def _normalize_days(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.lower() in {"string", "none", "null"}:
        return None
    return normalized


class ProcessRequest(BaseModel):
    days: Optional[str] = Field(
        default=None,
        description="Optional processing scope like 2026/01/14,2026/01/15. Leave empty to process all unfinished calls.",
        examples=["2026/01/14,2026/01/15"],
    )
    limit: Optional[int] = Field(
        default=None,
        description="Optional max number of calls to process. Use 0 for unlimited, or leave empty for the configured default.",
        examples=[0, 30],
    )
    force_reanalyze: bool = Field(
        default=False,
        description="When true, run analysis again even if analysis artifacts already exist.",
        examples=[False],
    )
    force_retranscribe: bool = Field(
        default=False,
        description="When true, run transcription again even if transcript artifacts already exist.",
        examples=[False],
    )
    generate_report_snapshots: Optional[bool] = Field(
        default=None,
        description="Optional override for writing snapshot report files during processing.",
        examples=[True, False],
    )

    @field_validator("days", mode="before")
    @classmethod
    def validate_days(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_days(value)

class SyncRequest(BaseModel):
    days: Optional[str] = Field(
        default=None,
        description="Optional PBX download scope like 2026/01/14,2026/01/15. Leave empty to sync all available dates.",
        examples=["2026/01/14,2026/01/15"],
    )

    @field_validator("days", mode="before")
    @classmethod
    def validate_days(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_days(value)

class JobResponse(BaseModel):
    job_id: str = Field(description="Unique job identifier.", examples=["job_20260320_101530_a1b2"])
    type: str = Field(
        description="Job type, for example `sync`, `process`, or `sync-and-process`.",
        examples=["process"],
    )
    status: JobStatus = Field(description="Current job execution status.")
    created_at: datetime = Field(description="UTC timestamp when the job was created.")
    started_at: Optional[datetime] = Field(default=None, description="UTC timestamp when execution started.")
    finished_at: Optional[datetime] = Field(default=None, description="UTC timestamp when execution finished.")
    result: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional result payload returned by the completed job.",
    )
    error: Optional[str] = Field(default=None, description="Error details when `status=failed`.")


class ReportFiltersQuery(BaseModel):
    date_from: Optional[date] = Field(
        default=None,
        description="Inclusive report start date in YYYY-MM-DD format.",
        examples=["2024-11-01"],
    )
    date_to: Optional[date] = Field(
        default=None,
        description="Inclusive report end date in YYYY-MM-DD format.",
        examples=["2024-11-30"],
    )
    manager_id: Optional[str] = Field(
        default=None,
        description="Filter by exact manager identifier.",
        examples=["petrenko_aa"],
    )
    role: Optional[str] = Field(
        default=None,
        description="Filter by exact manager role.",
        examples=["sales"],
    )
    direction: Optional[str] = Field(
        default=None,
        description="Filter by call direction.",
        examples=["incoming"],
    )
    intent: Optional[str] = Field(
        default=None,
        description="Filter by exact call intent label from analysis.",
        examples=["order_status"],
    )
    outcome: Optional[str] = Field(
        default=None,
        description="Filter by exact outcome label from analysis.",
        examples=["success"],
    )
    spam_only: bool = Field(
        default=False,
        description="When true, include only calls with spam probability >= threshold (default threshold is 0.7).",
        examples=[False],
    )
    effective_only: bool = Field(
        default=False,
        description="When true, include only calls marked as effective.",
        examples=[False],
    )

    @field_validator("manager_id", "role", "direction", "intent", "outcome", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class KeywordUpsertRequest(BaseModel):
    keyword_id: str = Field(
        min_length=1,
        description="Stable keyword identifier.",
        examples=["delivery"],
    )
    label: str = Field(
        min_length=1,
        description="Human-readable keyword label.",
        examples=["Delivery"],
    )
    category: str = Field(
        default="general",
        min_length=1,
        examples=["logistics"],
    )
    terms: List[str] = Field(
        default_factory=list,
        description="Phrases used to match the keyword.",
        examples=[["delivery", "order"]],
    )
    match_fields: List[str] = Field(
        default_factory=lambda: ["summary", "key_questions", "objections"],
        description="Analysis fields scanned for term matches.",
    )
    is_active: bool = True

    @field_validator("keyword_id", "label", "category", mode="before")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Value must not be empty")
        return normalized

    @field_validator("terms", "match_fields", mode="before")
    @classmethod
    def normalize_text_list(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("Expected a list")
        result: List[str] = []
        for item in value:
            normalized = str(item).strip()
            if normalized:
                result.append(normalized)
        return result


class KeywordSyncRequest(BaseModel):
    prune_missing: bool = Field(
        default=False,
        description="When true, delete Postgres keywords that are not present in the YAML file.",
    )


class KeywordGenerationRequest(BaseModel):
    date_from: Optional[date] = Field(
        default=None,
        description="Optional inclusive start date for candidate generation. Leave empty to scan across all available analyses.",
        examples=["2026-03-01"],
    )
    date_to: Optional[date] = Field(
        default=None,
        description="Optional inclusive end date for candidate generation. Leave empty to scan across all available analyses.",
        examples=["2026-03-20"],
    )
    manager_id: Optional[str] = Field(
        default=None,
        description="Optional exact manager id filter.",
        examples=["petrenko_aa"],
    )
    role: Optional[str] = Field(
        default=None,
        description="Optional exact role filter.",
        examples=["sales"],
    )
    direction: Optional[str] = Field(
        default=None,
        description="Optional exact direction filter.",
        examples=["incoming"],
    )
    intent: Optional[str] = Field(
        default=None,
        description="Optional exact intent filter.",
        examples=["order_status"],
    )
    outcome: Optional[str] = Field(
        default=None,
        description="Optional exact outcome filter.",
        examples=["success"],
    )
    spam_only: bool = Field(
        default=False,
        description="When true, consider only spam calls under configured threshold logic.",
    )
    effective_only: bool = Field(
        default=True,
        description="When true, consider only effective calls. Enabled by default for cleaner bootstrap candidates.",
    )
    include_summary: bool = Field(
        default=True,
        description="Use `summary` field from analyses.",
    )
    include_key_questions: bool = Field(
        default=True,
        description="Use `key_questions` field from analyses.",
    )
    include_objections: bool = Field(
        default=True,
        description="Use `objections` field from analyses.",
    )
    min_token_length: int = Field(
        default=4,
        ge=2,
        le=32,
        description="Minimum token length included into phrase extraction.",
    )
    max_ngram_words: int = Field(
        default=2,
        ge=1,
        le=3,
        description="Maximum number of words in generated candidate phrases.",
    )
    min_support_calls: int = Field(
        default=5,
        ge=1,
        le=1000000,
        description="Minimum number of unique calls that must contain the phrase.",
    )
    min_total_matches: int = Field(
        default=5,
        ge=1,
        le=1000000,
        description="Minimum total number of phrase matches across all scanned texts.",
    )
    max_candidates: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of generated candidates in response.",
    )
    exclude_existing_terms: bool = Field(
        default=True,
        description="Skip phrases that already exist in current keyword terms.",
    )

    @field_validator("manager_id", "role", "direction", "intent", "outcome", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def ensure_at_least_one_source_field(self) -> "KeywordGenerationRequest":
        if not (self.include_summary or self.include_key_questions or self.include_objections):
            raise ValueError(
                "At least one field source must be enabled: include_summary, include_key_questions, include_objections"
            )
        return self


class KeywordGenerationPublishCandidate(BaseModel):
    phrase: str = Field(
        min_length=1,
        description="Candidate phrase to publish into keyword catalog terms.",
        examples=["delivery delay"],
    )
    keyword_id: Optional[str] = Field(
        default=None,
        description="Optional explicit keyword_id. Generated automatically when omitted.",
        examples=["delivery_delay"],
    )
    label: Optional[str] = Field(
        default=None,
        description="Optional explicit label. Auto-derived from phrase when omitted.",
        examples=["Delivery Delay"],
    )
    category: Optional[str] = Field(
        default=None,
        description="Optional category override for this candidate.",
        examples=["generated"],
    )
    match_fields: Optional[List[str]] = Field(
        default=None,
        description="Optional match fields override for this candidate.",
        examples=[["summary", "key_questions"]],
    )
    is_active: Optional[bool] = Field(
        default=None,
        description="Optional activity flag override for this candidate.",
    )

    @field_validator("phrase", "keyword_id", "label", "category", mode="before")
    @classmethod
    def normalize_optional_scalar_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        return normalized

    @field_validator("match_fields", mode="before")
    @classmethod
    def normalize_match_fields(cls, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError("match_fields must be a list")
        result: List[str] = []
        for item in value:
            normalized = str(item).strip()
            if normalized:
                result.append(normalized)
        return result or None


class KeywordGenerationPublishRequest(BaseModel):
    candidates: List[KeywordGenerationPublishCandidate] = Field(
        min_length=1,
        description="Candidates to publish into keyword catalog.",
    )
    default_category: str = Field(
        default="generated",
        min_length=1,
        description="Default category for candidates that do not provide one.",
    )
    default_match_fields: List[str] = Field(
        default_factory=lambda: ["summary", "key_questions", "objections"],
        description="Default match fields for candidates that do not provide them.",
    )
    default_is_active: bool = Field(
        default=False,
        description="Default activation status for newly created keywords.",
    )
    materialize_after_publish: bool = Field(
        default=False,
        description="When true, runs keyword materialization immediately after publish.",
    )

    @field_validator("default_category", mode="before")
    @classmethod
    def normalize_default_category(cls, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("default_category must not be empty")
        return normalized

    @field_validator("default_match_fields", mode="before")
    @classmethod
    def normalize_default_match_fields(cls, value: Any) -> List[str]:
        if value is None:
            return ["summary", "key_questions", "objections"]
        if not isinstance(value, list):
            raise ValueError("default_match_fields must be a list")
        result: List[str] = []
        for item in value:
            normalized = str(item).strip()
            if normalized:
                result.append(normalized)
        if not result:
            raise ValueError("default_match_fields must contain at least one field")
        return result


class KeywordCatalogAnalysisRequest(BaseModel):
    keyword_ids: Optional[List[str]] = Field(
        default=None,
        description="Optional subset of keyword ids to analyze. Leave empty to analyze the current catalog slice.",
        examples=[["delivery", "refund"]],
    )
    include_inactive: bool = Field(
        default=False,
        description="Include inactive keywords in the AI analysis input.",
    )
    include_match_stats: bool = Field(
        default=True,
        description="Include current match statistics in the AI analysis input.",
    )
    max_keywords: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Maximum number of keywords to send into the AI analysis.",
    )
    max_groups: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of semantic groups the AI should produce.",
    )

    @field_validator("keyword_ids", mode="before")
    @classmethod
    def normalize_keyword_ids(cls, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError("keyword_ids must be a list")
        result: List[str] = []
        for item in value:
            normalized = str(item).strip()
            if normalized:
                result.append(normalized)
        return result or None

class PaginationQuery(BaseModel):
    limit: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Page size for paginated report items.",
        examples=[50],
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Zero-based offset for paginated report items.",
        examples=[0],
    )


class ManagerSortBy(str, Enum):
    manager_name = "manager_name"
    total_calls = "total_calls"
    spam_calls = "spam_calls"
    effective_calls = "effective_calls"
    total_duration_seconds = "total_duration_seconds"


class KeywordSortBy(str, Enum):
    label = "label"
    category = "category"
    matched_calls = "matched_calls"
    total_matches = "total_matches"


class KeywordCallSortBy(str, Enum):
    call_date = "call_date"
    match_count = "match_count"
    manager_name = "manager_name"
    intent = "intent"
    outcome = "outcome"


class KeywordManagersSortBy(str, Enum):
    manager_name = "manager_name"
    matched_calls = "matched_calls"
    total_matches = "total_matches"


class CustomerSortBy(str, Enum):
    customer_phone = "customer_phone"
    total_calls = "total_calls"
    spam_calls = "spam_calls"
    effective_calls = "effective_calls"
    total_duration_seconds = "total_duration_seconds"
    first_call_date = "first_call_date"
    last_call_date = "last_call_date"


class ManagersSortQuery(BaseModel):
    sort_by: ManagerSortBy = Field(
        default=ManagerSortBy.total_calls,
        description="Manager report sorting field.",
    )
    order: SortOrder = Field(default=SortOrder.desc, description="Sort direction.")


class KeywordsSortQuery(BaseModel):
    sort_by: KeywordSortBy = Field(
        default=KeywordSortBy.matched_calls,
        description="Keyword report sorting field.",
    )
    order: SortOrder = Field(default=SortOrder.desc, description="Sort direction.")


class KeywordCallsSortQuery(BaseModel):
    sort_by: KeywordCallSortBy = Field(
        default=KeywordCallSortBy.call_date,
        description="Keyword calls report sorting field.",
    )
    order: SortOrder = Field(default=SortOrder.desc, description="Sort direction.")


class KeywordManagersSortQuery(BaseModel):
    sort_by: KeywordManagersSortBy = Field(
        default=KeywordManagersSortBy.matched_calls,
        description="Keyword managers report sorting field.",
    )
    order: SortOrder = Field(default=SortOrder.desc, description="Sort direction.")


class CustomersSortQuery(BaseModel):
    sort_by: CustomerSortBy = Field(
        default=CustomerSortBy.total_calls,
        description="Customers report sorting field.",
    )
    order: SortOrder = Field(default=SortOrder.desc, description="Sort direction.")
