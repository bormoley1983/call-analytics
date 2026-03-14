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
  cli.py               # Entry point — commands: run (default), sync
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

## CLI Commands

```bash
# Run the full pipeline (default)
python src/cli.py
python src/cli.py run

# Download new recordings from PBX via SFTP
PBX_HOST=192.168.1.1 PBX_KEY_PATH=~/.ssh/pbx_key python src/cli.py sync
```

---

## Pipeline Phases

1. **Discovery** (`planner.py`) — finds `.wav` files under `calls_raw/`, optionally filtered by `DAYS` env var. Skips already-processed files unless `FORCE_RETRANSCRIBE` or `FORCE_REANALYZE` is set.
2. **Transcription** (`transcription.py`) — faster-whisper with VAD, Ukrainian language, brand name corrections per segment.
3. **Translation** (`llm_ollama.py`) — optional batched segment translation to Ukrainian via Ollama. Off by default (`FORCE_TRANSLATE_UK=0`).
4. **Analysis** (`llm_ollama.py`) — Ollama generates structured JSON per call. Text truncated to fit context window. Output validated by `ensure_analysis_schema`.
5. **Reports** (`reports.py`, `reports_html.py`) — JSON + HTML written to `out/`.
6. **Postgres sync** (`storage_postgres.py`) — optional, runs if `POSTGRES_DSN` is set.

---

## Adding Adapters

- **New PBX system:** create `adapters/pbx_<name>.py`, implement `parse_filename(name) -> Dict`. Pass it to `Pipeline` in `cli.py`.
- **New storage backend:** implement the `StoragePort` protocol from `ports/storage.py`.
- **New LLM:** implement the `LlmPort` protocol from `ports/llm.py`.

---

## Testing

```bash
# Run all tests inside the container
docker compose run --rm whisper_poc pytest

# Or locally with venv active
pytest tests/
```

Type-check:

```bash
mypy src/
```

---

## Build & Run

```bash
docker compose build
docker compose up

# Debug mode (attaches on port 5678)
docker compose run --rm whisper_debug
```
