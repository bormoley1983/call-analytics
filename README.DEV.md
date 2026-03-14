# Call Analytics — Developer Manual

## Development Environment

All development and production runs are containerized. No local Python install required for running the pipeline.

For IDE support (mypy, autocomplete), a local venv is recommended:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

mypy is configured in `mypy.ini` with `mypy_path = src`.

---

## Project Structure

```
src/
  cli.py               # Batch entry point — commands: run (default), sync, migrate-storage
  migrate_storage.py   # Universal storage migration tool (json <-> postgres)
  logging_config.py    # Centralised logging setup (LOG_LEVEL, LOG_FILE, etc.)
  api/
    app.py             # FastAPI app + router registration + lifespan
    schemas.py         # Pydantic request/response models
    job_store.py       # In-memory job state (id → status/result)
    runner.py          # Runs Pipeline / PbxSshDownloader as background tasks
    routes/
      health.py        # GET /health
      jobs.py          # POST /jobs/sync|sync-and-process|process, GET /jobs, GET /jobs/{id}
      reports.py       # GET /reports/overall|manager/{id}
      managers.py      # GET /managers
  core/
    pipeline.py        # Orchestrates transcription → analysis → reports
    planner.py         # File discovery and incremental filtering
    transcription.py   # Whisper transcription logic
    reports.py         # JSON aggregation (overall + per-manager)
    rules.py           # Validation, schema coercion, hashing, truncation
  adapters/
    audio_ffmpeg.py    # Audio normalization via ffmpeg
    llm_ollama.py      # Ollama translation + analysis
    pbx_asterisk.py    # FreePBX filename parser
    pbx_ssh.py         # SFTP downloader for PBX recordings
    storage_json.py    # JSON file storage (primary)
    storage_postgres.py# PostgreSQL sync (optional, triggered by POSTGRES_DSN)
    storage_qdrant.py  # Semantic search via Qdrant (not yet wired)
    reports_html.py    # HTML report renderer
  domain/
    config.py          # AppConfig dataclass + load_app_config()
    models.py          # Call/Transcript/Analysis dataclasses (reference)
  ports/
    audio.py           # AudioPort protocol
    llm.py             # LlmPort protocol
    pbx.py             # PbxPort protocol
    storage.py         # StoragePort protocol
config/
  managers.yaml        # Manager → phone number mapping
  brands.yaml          # Brand name corrections for transcription
  analysis.yaml        # Company context + Ollama analysis prompt
```

---

## Container Services

| Service | Profile | Port(s) | Description |
|---|---|---|---|
| `api` | _(default)_ | `127.0.0.1:8000` | REST API — always-on, `restart: unless-stopped` |
| `api_debug` | `debug-api` | `127.0.0.1:8000`, `5679` | API with debugpy (wait-for-client) |
| `batch` | `batch` | — | One-shot CLI processing run |
| `batch_debug` | `debug-batch` | `127.0.0.1:5678` | CLI run with debugpy (wait-for-client) |

All services share the same `call-analytics-base:cuda12` image built from `Dockerfile.base`.

```bash
# Build image
docker compose build

# Start API (default, stays running)
docker compose up -d

# One-shot batch processing run
docker compose --profile batch run --rm batch

# Debug API — attach debugger to 127.0.0.1:5679
docker compose --profile debug-api up api_debug

# Debug batch — attach debugger to 127.0.0.1:5678
docker compose --profile debug-batch run --rm batch_debug

# Tail API logs
docker compose logs -f api
```

---

## REST API

The API runs at `http://localhost:8000`. Available when the `api` service is up.

Interactive docs:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI schema**: `http://localhost:8000/openapi.json`

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check + Ollama reachability |
| `POST` | `/jobs/sync` | Download new recordings from PBX via SFTP |
| `POST` | `/jobs/sync-and-process` | Download missing recordings, then process only newly downloaded days |
| `POST` | `/jobs/process` | Transcribe + analyse (accepts `days`, `limit`, `force_reanalyze`, `force_retranscribe`) |
| `GET` | `/jobs` | List recent jobs |
| `GET` | `/jobs/{job_id}` | Poll status: `pending` → `running` → `done` / `failed` |
| `GET` | `/reports/overall` | Aggregated report from `out/report.json` |
| `GET` | `/reports/manager/{manager_id}` | Per-manager report from `out/reports/{id}.json` |
| `GET` | `/managers` | List all configured managers with extensions |

### Job concurrency

Only one process-like job can run at a time (`process` or `sync-and-process`).
A second `POST /jobs/process` or `POST /jobs/sync-and-process` while one is running returns `409 Conflict`.
`sync` jobs have no such restriction.

---

## CLI Commands (batch profile)

```bash
# Run the full pipeline
docker compose --profile batch run --rm batch python src/cli.py run

# Download new recordings from PBX via SFTP
docker compose --profile batch run --rm batch python src/cli.py sync

# Migrate stored data between backends
docker compose --profile batch run --rm batch \
  python src/cli.py migrate-storage --source json --target postgres --entities both --dry-run
```

Or directly with venv active (for local dev):

```bash
python src/cli.py run
PBX_HOST=192.168.1.1 PBX_KEY_PATH=~/.ssh/pbx_key python src/cli.py sync
python src/cli.py migrate-storage --source json --target postgres --entities both --postgres-dsn "postgresql://user:pass@localhost/calls"
```

### Storage migration command

`migrate-storage` is intentionally backend-agnostic.

Supported backends:
- `json`
- `postgres`

Supported entities:
- `transcripts`
- `analyses`
- `both`

Examples:

```bash
# JSON -> Postgres
python src/cli.py migrate-storage \
  --source json --target postgres --entities both \
  --postgres-dsn "postgresql://user:pass@localhost/calls"

# Postgres -> JSON
python src/cli.py migrate-storage \
  --source postgres --target json --entities analyses \
  --postgres-dsn "postgresql://user:pass@localhost/calls"

# Safe preview without writes
python src/cli.py migrate-storage \
  --source json --target postgres --entities both --dry-run \
  --postgres-dsn "postgresql://user:pass@localhost/calls"
```

Behavior flags:
- `--dry-run`: read + count only, no writes.
- `--stop-on-error`: abort on first malformed record/write failure.

---

## Pipeline Phases

1. **Discovery** (`planner.py`) — finds `.wav` files under `calls_raw/`, optionally filtered by `DAYS` env var. Skips already-processed files unless `FORCE_RETRANSCRIBE` or `FORCE_REANALYZE` is set. `PROCESS_LIMIT=0` means unlimited.
2. **Transcription** (`transcription.py`) — faster-whisper with VAD, Ukrainian language, brand name corrections per segment.
3. **Translation** (`llm_ollama.py`) — optional batched segment translation to Ukrainian via Ollama. Off by default (`FORCE_TRANSLATE_UK=0`).
4. **Analysis** (`llm_ollama.py`) — Ollama generates structured JSON per call. Text truncated to fit context window. Output validated by `ensure_analysis_schema`.
5. **Reports** (`reports.py`, `reports_html.py`) — JSON + HTML written to `out/`.
6. **Postgres sync** (`storage_postgres.py`) — optional, runs if `POSTGRES_DSN` is set.

---

## Logging

All services log to stdout. The API calls `setup_logging()` on startup via FastAPI lifespan; the CLI calls it in `__main__`.

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FORMAT` | `%(asctime)s %(levelname)-8s %(name)s: %(message)s` | Python format string |
| `LOG_FILE` | _(unset)_ | Write to a rotating file in addition to stdout |
| `LOG_MAX_BYTES` | `10485760` | Rotation threshold (10 MiB) |
| `LOG_BACKUP_COUNT` | `5` | Number of rotated files to keep |

Set variables in `config/.env`. Example for verbose debug with file output:

```env
LOG_LEVEL=DEBUG
LOG_FILE=/work/out/api.log
```

---

## Adding Adapters

- **New PBX system:** create `adapters/pbx_<name>.py`, implement `parse_filename(name) -> Dict`. Pass it to `Pipeline` in `cli.py` and `runner.py`.
- **New storage backend:** implement the `StoragePort` protocol from `ports/storage.py`.
- **New LLM:** implement the `LlmPort` protocol from `ports/llm.py`.

---

## Testing

```bash
# Run all tests inside the container
docker compose --profile batch run --rm batch pytest

# Or locally with venv active
pytest tests/
```

Type-check:

```bash
mypy src/
```
