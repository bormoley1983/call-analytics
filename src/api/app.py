from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import health, jobs, keywords, managers, reports
from logging_config import setup_logging

description = """
Internal API for Call Analytics.

**Typical workflow:**
1. `POST /jobs/sync` — download new recordings from PBX
2. `POST /jobs/process` — transcribe + analyse
3. `GET /jobs/{job_id}` — poll until `done`
4. `GET /reports/overall` — fetch results
"""

tags_metadata = [
    {"name": "health", "description": "Liveness and dependency checks."},
    {"name": "jobs", "description": "Trigger and monitor async jobs."},
    {"name": "reports", "description": "Fetch aggregated call reports."},
    {"name": "keywords", "description": "List keyword definitions used for reporting and mapping."},
    {"name": "managers", "description": "List configured managers and their extensions."},
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    yield

app = FastAPI(
    title="Call Analytics API",
    version="0.1.0",
    description=description,
    openapi_tags=tags_metadata,
    lifespan=lifespan,
)
app.include_router(health.router)
app.include_router(jobs.router)
app.include_router(reports.router)
app.include_router(keywords.router)
app.include_router(managers.router)
