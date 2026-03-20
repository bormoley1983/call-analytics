from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


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
