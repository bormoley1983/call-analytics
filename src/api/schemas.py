from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    done = "done"
    failed = "failed"


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
