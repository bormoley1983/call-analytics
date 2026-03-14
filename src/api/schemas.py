from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    done = "done"
    failed = "failed"

class ProcessRequest(BaseModel):
    days: Optional[str] = None          # "2026/01/14,2026/01/15"
    limit: Optional[int] = None
    force_reanalyze: bool = False
    force_retranscribe: bool = False

class SyncRequest(BaseModel):
    days: Optional[str] = None          # future: limit download scope

class JobResponse(BaseModel):
    job_id: str
    type: str                           # "sync" | "process"
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None