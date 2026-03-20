from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import health, jobs, keywords, managers, reports
from logging_config import setup_logging

description = """
Internal API for Call Analytics.

## Expected Flow
1. `POST /jobs/sync` (optional) to fetch fresh PBX recordings.
2. `POST /jobs/process` (or `POST /jobs/sync-and-process`) to run transcription and analysis.
3. Poll `GET /jobs/{job_id}` until `status` becomes `done` or `failed`.
4. Read aggregated analytics from `/reports/*`.
5. For keyword drill-down endpoints (`/reports/keywords/{keyword_id}/...`) with Postgres storage:
   - `POST /keywords/sync`
   - `POST /keywords/materialize`

## Defaults And Runtime Behavior
- Report source:
  - `POSTGRES_DSN` set -> Postgres adapters
  - `POSTGRES_DSN` missing -> JSON/YAML adapters
- Spam threshold:
  - Env var `SPAM_PROBABILITY_THRESHOLD`
  - Default `0.7`
- Report filters:
  - Date range is inclusive (`date_from`, `date_to`, format `YYYY-MM-DD`)
  - Text filters are optional; empty strings are treated as not provided
  - `spam_only=false`, `effective_only=false`
- Sorting and pagination defaults:
  - Managers: `sort_by=total_calls`, `order=desc`
  - Customers: `sort_by=total_calls`, `order=desc`
  - Keywords: `sort_by=matched_calls`, `order=desc`
  - Keyword calls: `limit=50`, `offset=0`, `sort_by=call_date`, `order=desc`
"""

tags_metadata = [
    {"name": "health", "description": "Liveness and dependency checks."},
    {"name": "jobs", "description": "Trigger and monitor async synchronization/processing jobs."},
    {"name": "reports", "description": "Fetch aggregated analytics and drill-down call reports."},
    {"name": "keywords", "description": "Manage keyword catalog and materialized keyword matches."},
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
