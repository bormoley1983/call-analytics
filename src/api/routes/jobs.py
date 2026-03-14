from fastapi import APIRouter, BackgroundTasks, HTTPException

from api import job_store
from api.runner import run_process, run_sync, run_sync_and_process
from api.schemas import JobResponse, JobStatus, ProcessRequest, SyncRequest

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _ensure_no_running_process_like_job() -> None:
    running = [
        j for j in job_store.list_jobs()
        if j.type in {"process", "sync-and-process"} and j.status == JobStatus.running
    ]
    if running:
        raise HTTPException(status_code=409, detail=f"Job {running[0].job_id} already running")


@router.post("/sync", response_model=JobResponse, status_code=202)
def trigger_sync(req: SyncRequest, background_tasks: BackgroundTasks):
    job = job_store.create_job("sync")
    background_tasks.add_task(run_sync, job.job_id, req)
    return job


@router.post("/process", response_model=JobResponse, status_code=202)
def trigger_process(req: ProcessRequest, background_tasks: BackgroundTasks):
    _ensure_no_running_process_like_job()
    job = job_store.create_job("process")
    background_tasks.add_task(run_process, job.job_id, req)
    return job


@router.post("/sync-and-process", response_model=JobResponse, status_code=202)
def trigger_sync_and_process(req: ProcessRequest, background_tasks: BackgroundTasks):
    _ensure_no_running_process_like_job()
    job = job_store.create_job("sync-and-process")
    background_tasks.add_task(run_sync_and_process, job.job_id, req)
    return job


@router.get("", response_model=list[JobResponse])
def list_jobs():
    return job_store.list_jobs()


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: str):
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
