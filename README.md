# Call Analytics

Production-first call analytics for FreePBX/Asterisk recordings.

The production flow is built around:
- PostgreSQL as the system of record
- Whisper for transcription
- Ollama for call analysis and keyword AI analysis
- FastAPI for jobs, reporting, keyword management, and drill-down APIs

JSON artifacts still exist for local development, migration, and optional snapshot exports, but they are not the production source of truth.

## Production Requirements

- Docker and Docker Compose
- NVIDIA Container Toolkit and a working GPU runtime
- Ollama reachable from the containers, with the required model loaded
- External PostgreSQL database reachable from the API container
- Valid config files under `config/`

Required production inputs:
- `config/.env`
- `config/managers.yaml`
- `config/brands.yaml`
- `config/analysis.yaml`
- `config/keywords.yaml`
- `POSTGRES_DSN` in `config/.env`

If `config/keywords.yaml` does not exist yet:

```bash
cp config/keywords.yaml.example config/keywords.yaml
```

## Quick Start

### 1. Prepare configuration

```bash
cp config/.env.example config/.env
cp config/managers.yaml.example config/managers.yaml
cp config/brands.yaml.example config/brands.yaml
cp config/analysis.yaml.example config/analysis.yaml
cp config/keywords.yaml.example config/keywords.yaml
```

Set at least these values in `config/.env`:

```env
POSTGRES_DSN=postgresql://user:pass@host:5432/call_analytics
OLLAMA_URL=http://host.docker.internal:11434
OLLAMA_MODEL=qwen3.5:27b
```

Optional PBX sync variables are described below.

### 2. Build and run the API

```bash
docker compose build
docker compose up -d api
```

The API is available at:
- `http://localhost:8000/docs`
- `http://localhost:8000/redoc`

## Production Flow

### Preferred daily flow

1. Optional: sync fresh recordings from PBX.
2. Run processing.
3. Wait for the job to finish.
4. Read `/reports/*`.

Examples:

```bash
# Optional PBX sync only
curl -X POST http://localhost:8000/jobs/sync \
  -H "Content-Type: application/json" \
  -d '{"days": "2026/03/19,2026/03/20"}'

# Process existing raw data already present under calls_raw/
curl -X POST http://localhost:8000/jobs/process \
  -H "Content-Type: application/json" \
  -d '{"days": "2026/03/19,2026/03/20", "limit": 30}'

# Or do both in one job
curl -X POST http://localhost:8000/jobs/sync-and-process \
  -H "Content-Type: application/json" \
  -d '{"days": "2026/03/19,2026/03/20", "limit": 30}'
```

Important behavior:
- `POST /jobs/sync` downloads files only. It does not run AI keyword analysis.
- `POST /jobs/process` runs transcription and analysis, stores results in Postgres, then automatically:
  - refreshes keywords
  - runs AI keyword catalog analysis
- `POST /jobs/sync-and-process` now uses the same `days` scope for both sync and processing.
- If `days` is omitted, `/jobs/sync-and-process` performs a full sync and then processes all eligible data.

### Poll the job

```bash
curl http://localhost:8000/jobs/<job_id>
```

A successful process-like job may include these sections in `result`:
- `keywords_refresh`
- `keyword_ai_analysis`

## Reports

Main production reports:
- `GET /reports/overall`
- `GET /reports/managers`
- `GET /reports/manager/{manager_id}`
- `GET /reports/customers`
- `GET /reports/customers/{customer_phone}`
- `GET /reports/keywords`
- `GET /reports/keywords/{keyword_id}`
- `GET /reports/keywords/{keyword_id}/calls`
- `GET /reports/keywords/{keyword_id}/trend`
- `GET /reports/keywords/{keyword_id}/managers`

All report responses now include a top-level `keyword_ai_analysis` field when a persisted AI keyword analysis exists in Postgres.

Example shape:

```json
{
  "total_calls": 3301,
  "effective_calls": 1820,
  "keyword_ai_analysis": {
    "analysis_id": "11111111-1111-1111-1111-111111111111",
    "created_at": "2026-03-20T12:00:00+00:00",
    "summary": "Top logistics and refund groups are overlapping.",
    "global_recommendations": [
      "Merge weak duplicate aliases."
    ],
    "groups_total": 4,
    "groups_returned": 4,
    "groups": [
      {
        "group_label": "Delivery / Shipment",
        "theme": "logistics overlap",
        "keywords": ["delivery", "shipment"],
        "primary_keyword_id": "delivery",
        "suggested_category": "logistics",
        "suggested_shared_terms": ["delivery", "shipment"],
        "suggested_actions": [],
        "rationale": "These keywords are strongly overlapping."
      }
    ]
  }
}
```

For keyword-specific endpoints, `keyword_ai_analysis.groups` is filtered down to groups relevant to that keyword.

## Keyword Operations

### Normal manual maintenance flow

Use the combined refresh endpoint:

```bash
curl -X POST http://localhost:8000/keywords/refresh \
  -H "Content-Type: application/json" \
  -d '{"prune_missing": false}'
```

This endpoint:
- syncs `config/keywords.yaml` into Postgres
- materializes keyword matches from existing analyses
- runs AI keyword analysis at the end

### Low-level maintenance endpoints

Available for admin/debugging use:
- `POST /keywords/sync`
- `POST /keywords/materialize`

These also trigger AI keyword analysis after they complete successfully.

### Keyword discovery and AI catalog analysis

Optional admin flows:
- `POST /keywords/generation/candidates`
- `POST /keywords/generation/publish`
- `POST /keywords/catalog/analysis`
- `GET /keywords/catalog/analyses`
- `GET /keywords/catalog/analyses/{analysis_id}`

## Configuration

### Core runtime

| Variable | Default | Production note |
|---|---|---|
| `POSTGRES_DSN` | unset | Required in production |
| `OLLAMA_URL` | `http://localhost:11434` | Must be reachable from container |
| `OLLAMA_MODEL` | `qwen3.5:27b` | Main analysis and keyword AI model |
| `SPAM_PROBABILITY_THRESHOLD` | `0.7` | Used in reports and keyword analysis |
| `AUTO_REFRESH_KEYWORDS` | `1` | Auto-refresh after successful processing |
| `AUTO_RUN_AI_KEYWORD_ANALYSIS` | `1` | Auto-run keyword catalog AI analysis after process-like flows |
| `GENERATE_REPORT_SNAPSHOTS` | `0` | Optional JSON/HTML snapshot export, not production source of truth |
| `PROCESS_LIMIT` | `30` | `0` means unlimited |
| `DAYS` | all eligible | Used by processing flow |

### PBX sync

Optional variables for `POST /jobs/sync` and `POST /jobs/sync-and-process`:
- `PBX_HOST`
- `PBX_PORT`
- `PBX_USER`
- `PBX_PASSWORD`
- `PBX_KEY_PATH`
- `PBX_KNOWN_HOSTS_PATH`
- `PBX_REMOTE_DIR`

The API expects recordings in FreePBX-style paths:

```text
calls_raw/YYYY/MM/DD/<dir>-<dst>-<src>-<YYYYMMDD>-<HHMMSS>-<uniqueid>.wav
```

## Production Notes

- Production should always run with `POSTGRES_DSN` configured.
- Invalid or missing keyword YAML now fails loudly in admin/reporting flows instead of silently appearing as an empty catalog.
- Keyword drill-down endpoints require materialized Postgres keyword data.
- Snapshot files in `out/` are optional exports only.

## Legacy JSON Mode

JSON storage and JSON/YAML read paths remain in the repository for:
- local development
- migration from older deployments
- optional snapshot exports
- compatibility testing

They are not the recommended production mode.

If you still have historical JSON data, migrate it into Postgres before using the production reporting flow.

## Development and Testing

Developer-oriented setup and architecture notes live in [README.DEV.md](README.DEV.md).
