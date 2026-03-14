from fastapi import APIRouter, BackgroundTasks, HTTPException

from api import job_store
from api.runner import run_process, run_sync
from api.schemas import JobResponse, JobStatus, ProcessRequest, SyncRequest

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.post("/sync", response_model=JobResponse, status_code=202)
def trigger_sync(req: SyncRequest, background_tasks: BackgroundTasks):
    job = job_store.create_job("sync")
    background_tasks.add_task(run_sync, job.job_id, req)
    return job

@router.post("/process", response_model=JobResponse, status_code=202)
def trigger_process(req: ProcessRequest, background_tasks: BackgroundTasks):
    # Reject if a process job is already running
    running = [j for j in job_store.list_jobs() if j.type == "process" and j.status == JobStatus.running]
    if running:
        raise HTTPException(status_code=409, detail=f"Process job {running[0].job_id} already running")
    job = job_store.create_job("process")
    background_tasks.add_task(run_process, job.job_id, req)
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