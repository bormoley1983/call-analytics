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
    force_reanalyze: bool = False
    force_retranscribe: bool = False
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
    job_id: str
    type: str                           # "sync" | "process"
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


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
    manager_id: Optional[str] = None
    role: Optional[str] = None
    direction: Optional[str] = None
    intent: Optional[str] = None
    outcome: Optional[str] = None
    spam_only: bool = False
    effective_only: bool = False

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
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


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


class ManagersSortQuery(BaseModel):
    sort_by: ManagerSortBy = ManagerSortBy.total_calls
    order: SortOrder = SortOrder.desc


class KeywordsSortQuery(BaseModel):
    sort_by: KeywordSortBy = KeywordSortBy.matched_calls
    order: SortOrder = SortOrder.desc


class KeywordCallsSortQuery(BaseModel):
    sort_by: KeywordCallSortBy = KeywordCallSortBy.call_date
    order: SortOrder = SortOrder.desc


class KeywordManagersSortQuery(BaseModel):
    sort_by: KeywordManagersSortBy = KeywordManagersSortBy.matched_calls
    order: SortOrder = SortOrder.desc
