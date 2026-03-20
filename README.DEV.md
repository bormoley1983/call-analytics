# Call Analytics — Developer Manual

## Scope

This manual is aimed at contributors working on the Postgres-first production system.

Current operating model:
- PostgreSQL is the production system of record.
- FastAPI jobs and reports are designed around persisted Postgres data.
- JSON storage remains only for local development, migration, and legacy compatibility.
- Qdrant is not part of the active production stack.

## Local Development Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

Optional future-only dependency set for the Qdrant plan:

```bash
pip install -r requirements-qdrant.txt
```

Useful checks:

```bash
pytest
mypy --no-incremental src tests
```

## Requirements Files

- `requirements.txt`: production runtime dependencies for the active Postgres/Ollama/FastAPI stack
- `requirements-dev.txt`: local dev tooling and typing/test dependencies
- `requirements-qdrant.txt`: optional dependency file for the future Qdrant plan only

## Production Runtime Model

### Source of truth

Production writes directly into Postgres tables, primarily:
- `transcripts`
- `analyses`
- `keywords`
- `keyword_aliases`
- `call_keywords`
- `keyword_materialization_state`
- `keyword_ai_analyses`
- `keyword_ai_analysis_items`

### Default API flow

1. `POST /jobs/sync` optionally downloads PBX recordings into `calls_raw/`
2. `POST /jobs/process` or `POST /jobs/sync-and-process` runs the transcription and analysis pipeline
3. The job runner then performs post-processing orchestration:
   - keyword refresh
   - keyword AI analysis
4. `/reports/*` reads persisted data and attaches the latest persisted `keyword_ai_analysis`

Important behavior:
- `/jobs/sync` is download-only and does not run AI keyword analysis
- `/jobs/sync-and-process` uses the same `days` filter for both sync and processing
- report endpoints are read-side APIs and do not trigger write-side flows

## Project Structure

```text
src/
  api/
    app.py                     FastAPI app and top-level OpenAPI description
    job_store.py               in-memory async job registry
    runner.py                  background job orchestration
    schemas.py                 request/response/query models
    routes/
      health.py                health endpoint
      jobs.py                  sync, process, sync-and-process jobs
      reports.py               Postgres-backed online reports
      keywords.py              keyword catalog, refresh, sync, materialize
      keywords_ai.py           AI keyword catalog analysis history
      keywords_generation.py   rule-based keyword candidate generation/publish
      managers.py              configured managers reference
  core/
    pipeline.py                transcription + translation + analysis pipeline
    reporting_service.py       overall/managers/customers aggregation logic
    keywords_service.py        keyword report aggregation logic
    keywords_sync.py           YAML -> Postgres keyword sync
    keywords_materialize.py    analyses -> call_keywords materialization
    keywords_refresh.py        combined keyword refresh service
    keywords_ai.py             AI analysis input preparation
    keywords_ai_runtime.py     shared runtime trigger for post-flow AI analysis
  adapters/
    storage_postgres.py                Postgres storage and DDL
    reporting_postgres.py              Postgres reporting source
    keywords_postgres.py               Postgres keyword storage and drill-downs
    keyword_ai_analysis_postgres.py    persisted AI keyword analysis history
    llm_ollama.py                      Ollama analysis and keyword AI prompts
    pbx_ssh.py                         PBX download adapter
    storage_json.py                    legacy/dev-only JSON storage
    reporting_json.py                  legacy/dev-only JSON reporting
    keywords_yaml.py                   YAML keyword catalog loader
    storage_qdrant.py                  future scaffold, not active
```

## Pipeline and Job Boundaries

### `Pipeline.run()`

The pipeline itself is responsible for:
- discovery
- audio normalization
- transcription
- optional translation
- call analysis
- persistence via the configured storage backend
- optional snapshot report export when `GENERATE_REPORT_SNAPSHOTS=1`

The pipeline is not responsible for:
- keyword refresh orchestration
- keyword AI analysis orchestration
- online reporting

Those happen in `api/runner.py` after successful process-like jobs.

### Post-process orchestration

After a successful process job, the runner may do:
- `refresh_keywords_data(...)`
- `run_keyword_ai_analysis_once(...)`

This separation keeps processing/storage and downstream enrichment easier to reason about.

## Production-Oriented API Areas

### Jobs

- `POST /jobs/sync`
- `POST /jobs/process`
- `POST /jobs/sync-and-process`
- `GET /jobs`
- `GET /jobs/{job_id}`

### Reports

- `GET /reports/overall`
- `GET /reports/managers`
- `GET /reports/manager/{manager_id}`
- `GET /reports/customers`
- `GET /reports/customers/{customer_phone}`
- `GET /reports/keywords`
- `GET /reports/keywords/{keyword_id}`
- keyword drill-down endpoints under `/reports/keywords/{keyword_id}/*`

Every report response may include `keyword_ai_analysis` when Postgres analysis history is available.

### Keywords

Normal admin flow:
- `POST /keywords/refresh`

Low-level maintenance:
- `POST /keywords/sync`
- `POST /keywords/materialize`

Optional advanced flows:
- `POST /keywords/generation/candidates`
- `POST /keywords/generation/publish`
- `POST /keywords/catalog/analysis`
- `GET /keywords/catalog/analyses`
- `GET /keywords/catalog/analyses/{analysis_id}`

## JSON Mode

JSON mode is kept for:
- local debugging
- historical migration
- compatibility testing
- optional snapshot export validation

It is not the production target.

When editing documentation, examples, or test instructions:
- lead with Postgres-first behavior
- treat JSON mode as a clearly separate legacy/dev-only path
- avoid describing JSON as the primary runtime path

## Testing Strategy

Current local checks:

```bash
pytest
mypy --no-incremental src tests
PYTHONPATH=src ./.venv/bin/python -c "import api.app as app; print(bool(app.app))"
```

Online/integration plans are tracked separately in the remaining `DEVPLAN_*` files.
