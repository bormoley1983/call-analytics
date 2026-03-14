# Call Analytics

Processes FreePBX/Asterisk call recordings: normalizes audio, transcribes with Whisper, analyzes with Ollama LLM, and generates reports (JSON + HTML).

## Requirements

- Docker + NVIDIA container toolkit (GPU required)
- Ollama running on the host with a model loaded (default: `qwen3.5:27b`)

## Quick Start

### 1. Prepare config files

```bash
cp config/managers.yaml.example config/managers.yaml
cp config/brands.yaml.example config/brands.yaml
cp config/analysis.yaml.example config/analysis.yaml
```

Edit each file for your company's managers, brand names, and analysis prompt.

### 2. Place recordings

Call recordings must follow FreePBX naming convention:

```
calls_raw/YYYY/MM/DD/<dir>-<dst>-<src>-<YYYYMMDD>-<HHMMSS>-<uniqueid>.wav
```

### 3. Build and run

```bash
docker compose build
docker compose up
```

Outputs are written to `out/`:
- `out/report.json` / `out/report.html` — overall summary
- `out/report_by_manager.json` / `out/report_by_manager.html` — per-manager breakdown
- `out/transcripts/<call_id>.json` — raw transcripts
- `out/analysis/<call_id>.json` — per-call analysis

---

## Configuration

### Environment Variables

All variables are optional — defaults are shown.

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_MODEL` | `qwen3.5:27b` | Model to use for analysis |
| `WHISPER_MODEL` | `large-v3-turbo` | faster-whisper model |
| `WHISPER_DEVICE` | `cuda` | `cuda` or `cpu` |
| `WHISPER_COMPUTE_TYPE` | `float16` | `float16`, `int8`, etc. |
| `DAYS` | _(all)_ | Comma-separated `YYYY/MM/DD` to process specific days only |
| `PROCESS_LIMIT` | `30` | Max files per run |
| `MIN_BYTES` | `20000` | Skip files smaller than this |
| `MIN_SECONDS` | `1.0` | Skip calls shorter than this |
| `FORCE_RETRANSCRIBE` | `0` | Set to `1` to re-transcribe already processed files |
| `FORCE_REANALYZE` | `0` | Set to `1` to re-analyze already analyzed files |
| `FORCE_TRANSLATE_UK` | `0` | Set to `1` to translate transcripts to Ukrainian |
| `SPAM_PROBABILITY_THRESHOLD` | `0.7` | Calls above this are counted as spam |
| `POSTGRES_DSN` | _(unset)_ | If set, syncs results to PostgreSQL after each run |

### Process specific days

Uncomment and edit `DAYS` in `compose.yaml`, or pass it inline:

```bash
DAYS=2024/10/14,2024/10/15 docker compose up
```

---

## PBX Auto-Download (optional)

Download new recordings from the PBX via SFTP before processing:

```bash
PBX_HOST=192.168.1.1 \
PBX_KEY_PATH=~/.ssh/pbx_key \
PBX_REMOTE_DIR=/var/spool/asterisk/monitor \
python src/cli.py sync
```

The host key must already be in `~/.ssh/known_hosts`.

---

## PostgreSQL Integration (optional)

Set `POSTGRES_DSN` to automatically sync results after each run:

```bash
POSTGRES_DSN=postgresql://user:pass@localhost/calls docker compose up
```

Two tables are created automatically: `transcripts` and `analyses`.

---

## Debugging

Use the `whisper_debug` service to attach a remote debugger (port 5678):

```bash
docker compose run --rm whisper_debug
```

---

## Troubleshooting

**Ollama not reachable** — Ollama runs on the host; the container connects via `host.docker.internal:11434`. Make sure Ollama is running and the model is pulled before starting the container.

**No files processed** — Check that recordings exist under `calls_raw/YYYY/MM/DD/` and are larger than `MIN_BYTES`.

**CUDA out of memory** — Reduce `WHISPER_MODEL` (e.g. `medium`) or switch `WHISPER_COMPUTE_TYPE` to `int8`.

```bash
docker compose logs
```
