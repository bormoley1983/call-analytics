# Call Analytics

Processes FreePBX/Asterisk call recordings: normalizes audio, transcribes with Whisper, analyzes with Ollama LLM, and generates reports (JSON + HTML).

## Requirements

- Docker + NVIDIA container toolkit (GPU required)
- Ollama running on the host with a model loaded (default: `qwen3.5:27b`)

## Quick Start

### 1. Prepare config files

```bash
cp managers.yaml.example managers.yaml
cp brands.yaml.example brands.yaml
cp analysis.yaml.example analysis.yaml
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
docker compose up -d
```

The API starts automatically. Outputs are written to out:
- report.json / report.html — overall summary
- report_by_manager.json / report_by_manager.html — per-manager breakdown
- `out/transcripts/<call_id>.json` — raw transcripts
- `out/analysis/<call_id>.json` — per-call analysis

---

## Container Services

| Service | Profile | Port | Description |
|---|---|---|---|
| `api` | _(default)_ | `127.0.0.1:8000` | REST API — always-on, `restart: unless-stopped` |
| `api_debug` | `debug-api` | `127.0.0.1:8000` + `5679` | API with debugpy attached |
| `batch` | `batch` | — | One-shot CLI processing run |
| `batch_debug` | `debug-batch` | `127.0.0.1:5678` | CLI run with debugpy attached |

All services share the same `call-analytics-base:cuda12` image built from Dockerfile.base.

```bash
# Start API (default)
docker compose up -d

# Run a one-shot batch job
docker compose --profile batch run --rm batch

# Debug API (attach debugger to port 5679)
docker compose --profile debug-api up api_debug

# Debug batch (attach debugger to port 5678)
docker compose --profile debug-batch run --rm batch_debug
```

---

## REST API

The API runs at `http://localhost:8000`. Interactive docs available at:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check + Ollama status |
| `POST` | `/jobs/sync` | Download new recordings from PBX via SFTP |
| `POST` | `/jobs/sync-and-process` | Download missing recordings, then process only newly downloaded days |
| `POST` | `/jobs/process` | Transcribe + analyse recordings |
| `GET` | `/jobs` | List recent jobs |
| `GET` | `/jobs/{job_id}` | Poll job status (`pending` → `running` → `done`/`failed`) |
| `GET` | `/reports/overall` | Aggregated report |
| `GET` | `/reports/manager/{manager_id}` | Per-manager report |
| `GET` | `/managers` | List configured managers and their extensions |

### Typical workflow

```bash
# 1. Download new recordings from PBX
curl -X POST http://localhost:8000/jobs/sync

# 2. Trigger processing (optionally scope to specific days)
curl -X POST http://localhost:8000/jobs/process \
  -H "Content-Type: application/json" \
  -d '{"days": "2026/03/13,2026/03/14", "limit": 50}'

# 3. Poll until done
curl http://localhost:8000/jobs/<job_id>

# 4. Fetch results
curl http://localhost:8000/reports/overall
```

Or use one combined job:

```bash
curl -X POST http://localhost:8000/jobs/sync-and-process \
    -H "Content-Type: application/json" \
    -d '{"limit": 0, "force_reanalyze": false, "force_retranscribe": false}'
```

### Scheduled nightly run (cron)

```cron
0 22 * * * curl -sS -X POST http://localhost:8000/jobs/sync && sleep 60 && curl -sS -X POST http://localhost:8000/jobs/process
```

---

## Configuration

### Environment Variables

All variables are optional — defaults are shown. Set them in .env.

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_MODEL` | `qwen3.5:27b` | Model to use for analysis |
| `WHISPER_MODEL` | `large-v3-turbo` | faster-whisper model |
| `WHISPER_DEVICE` | `cuda` | `cuda` or `cpu` |
| `WHISPER_COMPUTE_TYPE` | `float16` | `float16`, `int8`, etc. |
| `DAYS` | _(all)_ | Comma-separated `YYYY/MM/DD` — process specific days only |
| `PROCESS_LIMIT` | `30` | Max files per run |
| `MIN_BYTES` | `20000` | Skip files smaller than this |
| `MIN_SECONDS` | `1.0` | Skip calls shorter than this |
| `FORCE_RETRANSCRIBE` | `0` | `1` to re-transcribe already processed files |
| `FORCE_REANALYZE` | `0` | `1` to re-analyze already analyzed files |
| `FORCE_TRANSLATE_UK` | `0` | `1` to translate transcripts to Ukrainian |
| `SPAM_PROBABILITY_THRESHOLD` | `0.7` | Calls above this are counted as spam |
| `POSTGRES_DSN` | _(unset)_ | If set, syncs results to PostgreSQL after each run |

Notes:
- `limit=0` in `/jobs/process` and `/jobs/sync-and-process` means unlimited processing (no cap).

### Logging

All logs go to stdout. Set these in .env to adjust behaviour:

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FORMAT` | `%(asctime)s %(levelname)-8s %(name)s: %(message)s` | Python logging format string |
| `LOG_FILE` | _(unset)_ | Path to a rotating log file (in addition to stdout) |
| `LOG_MAX_BYTES` | `10485760` | Max bytes per log file before rotation (10 MiB) |
| `LOG_BACKUP_COUNT` | `5` | Number of rotated log files to keep |

```bash
# View live API logs
docker compose logs -f api
```

---

## PBX Auto-Download (optional)

Set these in .env to enable the `/jobs/sync` endpoint:

### Key-based PBX access

Recommended setup is a dedicated read-only SFTP user with no interactive shell.

Server-side setup on PBX:

    sudo adduser --system --group --home /home/monitor_reader monitor_reader
    sudo passwd -l monitor_reader

    sudo mkdir -p /home/monitor_reader/.ssh
    sudo chmod 700 /home/monitor_reader/.ssh
    sudo chown monitor_reader:monitor_reader /home/monitor_reader/.ssh

    sudo nano /home/monitor_reader/.ssh/authorized_keys
    sudo chmod 600 /home/monitor_reader/.ssh/authorized_keys
    sudo chown monitor_reader:monitor_reader /home/monitor_reader/.ssh/authorized_keys

    sudo usermod -aG asterisk monitor_reader
    sudo chmod 750 /var/spool/asterisk/monitor
    sudo chmod -R g+rX /var/spool/asterisk/monitor

Restrict the account to SFTP only in sshd_config:

    Match User monitor_reader
        ForceCommand internal-sftp
        PasswordAuthentication no
        PubkeyAuthentication yes
        PermitTTY no
        X11Forwarding no
        AllowTcpForwarding no

Then restart SSH:

    sudo systemctl restart ssh

Client-side key install and test:

    ssh-copy-id -i pbx_ed25519.pub monitor_reader@192.168.10.202
    sftp -i /home/admaccess/call-analytics/config/ssh/pbx_ed25519 monitor_reader@192.168.10.202

Inside SFTP, verify access with:

    ls /var/spool/asterisk/monitor

Use this env configuration:

    PBX_USER=monitor_reader
    PBX_AUTH_MODE=key
    PBX_PASSWORD=
    PBX_KEY_PATH=/work/config/ssh/pbx_ed25519
    PBX_KNOWN_HOSTS_PATH=/work/config/ssh/known_hosts
    PBX_REMOTE_DIR=/var/spool/asterisk/monitor

| Variable | Description |
|---|---|
| `PBX_HOST` | PBX hostname or IP |
| `PBX_USER` | SSH user (default: `asterisk`) |
| `PBX_KEY_PATH` | Path to SSH private key |
| `PBX_REMOTE_DIR` | Remote recordings directory (default: `/var/spool/asterisk/monitor`) |
| `PBX_AUTH_MODE` | auto | key | password  |
| `PBX_PASSWORD` | used when auth mode is password  |
| `PBX_PORT` | SSH port  |
| `PBX_KNOWN_HOSTS_PATH` | known_hosts file path inside container  |

The host key must already be in `~/.ssh/known_hosts`.

---

## PostgreSQL Integration (optional)

Set `POSTGRES_DSN` to automatically sync results after each run:

```
POSTGRES_DSN=postgresql://user:pass@localhost/calls
```

Two tables are created automatically: `transcripts` and `analyses`.

### Storage Migration (JSON <-> PostgreSQL)

The project includes a universal migration command for moving persisted data between supported storages.

Supported backends:
- `json`
- `postgres`

Supported entities:
- `transcripts`
- `analyses`
- `both`

Run migration from existing JSON files to PostgreSQL:

```bash
python src/cli.py migrate-storage \
    --source json \
    --target postgres \
    --entities both \
    --postgres-dsn "postgresql://user:pass@localhost/calls"
```

Dry-run example:

```bash
python src/cli.py migrate-storage \
    --source json \
    --target postgres \
    --entities both \
    --dry-run \
    --postgres-dsn "postgresql://user:pass@localhost/calls"
```

Reverse migration (PostgreSQL back to JSON):

```bash
python src/cli.py migrate-storage \
    --source postgres \
    --target json \
    --entities both \
    --postgres-dsn "postgresql://user:pass@localhost/calls"
```

---

## Troubleshooting

**Ollama not reachable** — Ollama runs on the host; the container connects via `host.docker.internal:11434`. Make sure Ollama is running and the model is pulled before starting.

**No files processed** — Check that recordings exist under `calls_raw/YYYY/MM/DD/` and are larger than `MIN_BYTES`.

**CUDA out of memory** — Reduce `WHISPER_MODEL` (e.g. `medium`) or switch `WHISPER_COMPUTE_TYPE` to `int8`.

```bash
docker compose logs -f api
```
