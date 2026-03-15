import uuid
from datetime import datetime, timezone
from threading import RLock
from typing import Dict, Optional

from api.schemas import JobResponse, JobStatus

_jobs: Dict[str, JobResponse] = {}
_lock = RLock()

def create_job(type: str) -> JobResponse:
    with _lock:
        job = JobResponse(
            job_id=str(uuid.uuid4()),
            type=type,
            status=JobStatus.pending,
            created_at=datetime.now(timezone.utc),
        )
        _jobs[job.job_id] = job
        return job

def get_job(job_id: str) -> Optional[JobResponse]:
    with _lock:
        return _jobs.get(job_id)

def list_jobs(limit: int = 50) -> list:
    with _lock:
        return sorted(_jobs.values(), key=lambda j: j.created_at, reverse=True)[:limit]

def update_job(job_id: str, **kwargs) -> None:
    with _lock:
        job = _jobs[job_id]
        for k, v in kwargs.items():
            setattr(job, k, v)

def create_process_like_job_if_none_running(type: str) -> JobResponse | None:
    with _lock:
        running = [
            j for j in _jobs.values()
            if j.type in {"process", "sync-and-process"} and j.status in {JobStatus.pending, JobStatus.running}
        ]
        if running:
            return None
        job = JobResponse(
            job_id=str(uuid.uuid4()),
            type=type,
            status=JobStatus.pending,
            created_at=datetime.now(timezone.utc),
        )
        _jobs[job.job_id] = job
        return job