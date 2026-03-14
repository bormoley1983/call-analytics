import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from api.schemas import JobResponse, JobStatus

_jobs: Dict[str, JobResponse] = {}

def create_job(type: str) -> JobResponse:
    job = JobResponse(
        job_id=str(uuid.uuid4()),
        type=type,
        status=JobStatus.pending,
        created_at=datetime.now(timezone.utc),
    )
    _jobs[job.job_id] = job
    return job

def get_job(job_id: str) -> Optional[JobResponse]:
    return _jobs.get(job_id)

def list_jobs(limit: int = 50) -> list:
    return sorted(_jobs.values(), key=lambda j: j.created_at, reverse=True)[:limit]

def update_job(job_id: str, **kwargs) -> None:
    job = _jobs[job_id]
    for k, v in kwargs.items():
        setattr(job, k, v)