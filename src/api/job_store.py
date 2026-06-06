import uuid
from datetime import datetime, timezone
from threading import RLock
from typing import Dict, Optional

from api.schemas import JobResponse, JobStatus

_jobs: Dict[str, JobResponse] = {}
_lock = RLock()
_ACTIVE_JOB_STATUSES = {JobStatus.pending, JobStatus.running}


def _create_job_locked(type: str) -> JobResponse:
    job = JobResponse(
        job_id=str(uuid.uuid4()),
        type=type,
        status=JobStatus.pending,
        created_at=datetime.now(timezone.utc),
    )
    _jobs[job.job_id] = job
    return job


def _has_active_job(types: set[str]) -> bool:
    return any(
        job.type in types and job.status in _ACTIVE_JOB_STATUSES
        for job in _jobs.values()
    )


def create_job(type: str) -> JobResponse:
    with _lock:
        return _create_job_locked(type)


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


def create_sync_job_if_none_running() -> JobResponse | None:
    with _lock:
        if _has_active_job({"sync", "sync-and-process"}):
            return None
        return _create_job_locked("sync")


def create_process_like_job_if_none_running(type: str) -> JobResponse | None:
    with _lock:
        if _has_active_job({"process", "sync-and-process"}):
            return None
        return _create_job_locked(type)


def create_sync_and_process_job_if_none_running() -> JobResponse | None:
    with _lock:
        if _has_active_job({"sync", "process", "sync-and-process"}):
            return None
        return _create_job_locked("sync-and-process")
