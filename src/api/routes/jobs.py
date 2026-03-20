from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path

from api import job_store
from api.runner import run_process, run_sync, run_sync_and_process
from api.schemas import JobResponse, ProcessRequest, SyncRequest

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.post(
    "/sync",
    response_model=JobResponse,
    status_code=202,
    summary="Start sync job",
    description=(
        "Queues a background PBX synchronization job.\n\n"
        "**Example body**\n"
        "```json\n"
        "{\"days\": \"2026/03/18,2026/03/19\"}\n"
        "```"
    ),
)
def trigger_sync(req: SyncRequest, background_tasks: BackgroundTasks):
    job = job_store.create_job("sync")
    background_tasks.add_task(run_sync, job.job_id, req)
    return job


@router.post(
    "/process",
    response_model=JobResponse,
    status_code=202,
    summary="Start process job",
    description=(
        "Queues a background processing job (transcription + analysis).\n\n"
        "Only one process-like job can run at a time.\n\n"
        "**Defaults**\n"
        "- `days=null` -> process all unfinished calls\n"
        "- `limit=null` -> use configured default\n"
        "- `force_reanalyze=false`\n"
        "- `force_retranscribe=false`\n"
        "- `generate_report_snapshots=null`\n\n"
        "**Example body**\n"
        "```json\n"
        "{\"days\": \"2026/03/19\", \"limit\": 30, \"force_reanalyze\": false}\n"
        "```"
    ),
    responses={
        409: {
            "description": "A process-like job is already running.",
            "content": {
                "application/json": {"example": {"detail": "A process-like job is already running"}}
            },
        }
    },
)
def trigger_process(req: ProcessRequest, background_tasks: BackgroundTasks):
    job = job_store.create_process_like_job_if_none_running("process")
    if job is None:
        raise HTTPException(status_code=409, detail="A process-like job is already running")
    background_tasks.add_task(run_process, job.job_id, req)
    return job

@router.post(
    "/sync-and-process",
    response_model=JobResponse,
    status_code=202,
    summary="Start sync and process chain",
    description=(
        "Queues one background job that first syncs PBX records and then processes them.\n\n"
        "Uses the same request defaults as `/jobs/process`."
    ),
    responses={
        409: {
            "description": "A process-like job is already running.",
            "content": {
                "application/json": {"example": {"detail": "A process-like job is already running"}}
            },
        }
    },
)
def trigger_sync_and_process(req: ProcessRequest, background_tasks: BackgroundTasks):
    job = job_store.create_process_like_job_if_none_running("sync-and-process")
    if job is None:
        raise HTTPException(status_code=409, detail="A process-like job is already running")
    background_tasks.add_task(run_sync_and_process, job.job_id, req)
    return job


@router.get(
    "",
    response_model=list[JobResponse],
    summary="List jobs",
    description="Returns all known jobs in reverse chronological order.",
)
def list_jobs():
    return job_store.list_jobs()


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job status",
    description="Returns details for one job id. Poll this endpoint until status is `done` or `failed`.",
    responses={
        404: {
            "description": "Job id is not known.",
            "content": {"application/json": {"example": {"detail": "Job not found"}}},
        }
    },
)
def get_job(job_id: Annotated[str, Path(description="Job identifier returned by job trigger endpoints.")]):
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
