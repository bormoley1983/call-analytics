# Call Analytics — Architecture, Patterns & Design Decisions

Date: 2026-08-20
Status: current (reflects the q38 antipattern-fix branch, including the review follow-up M1–M4 closures).

This document summarizes how the system is structured, which architectural
patterns are used, and the reasoning behind the key design decisions. It is a
companion to `README.DEV.md` (operational manual) and the `DEVPLAN_*` files
(issue tracking).

---

## 1. System overview

Call Analytics ingests PBX call recordings, transcribes them (faster-whisper or
Canary), optionally translates them, runs LLM-based analysis (Ollama), and
serves aggregated reports through a FastAPI service.

Production operating model:

- **PostgreSQL is the system of record** (`transcripts`, `analyses`, `keywords`,
  `keyword_aliases`, `call_keywords`, `keyword_materialization_state`,
  `keyword_ai_analyses`, `keyword_ai_analysis_items`).
- JSON/YAML adapters exist only for local development, migration, and legacy
  compatibility — never the production path.
- Qdrant is a future scaffold (`adapters/storage_qdrant.py`), not active.

Main runtime flows:

1. `POST /jobs/sync` — download PBX recordings into `calls_raw/` (download only).
2. `POST /jobs/process` / `/jobs/sync-and-process` — transcription + analysis pipeline.
3. Post-processing orchestration in the runner: keyword refresh, keyword AI analysis.
4. Optional `POST /jobs/export-snapshots` — snapshot export.
5. `GET /reports/*` — read-side reporting over persisted data.

---

## 2. Hexagonal (ports & adapters) architecture

The codebase under `src/` is organized as a hexagon with strict, test-enforced
layering:

```
                 ┌────────────────────────────┐
                 │            api/            │   ← edge / composition root
                 │  FastAPI app, routes,      │     (may import everything)
                 │  runner, deps, job_store   │
                 └──────────────┬─────────────┘
                                │ constructs & injects
        ┌───────────────────────┼────────────────────────┐
        ▼                       ▼                        ▼
┌──────────────┐        ┌──────────────┐        ┌──────────────────┐
│    core/     │        │   ports/     │        │   adapters/      │
│ business     │◄──────►│ Protocols    │◄──────►│ concrete impls:  │
│ logic, pure  │        │ (interfaces) │        │ Postgres, JSON,  │
└──────┬───────┘        └──────────────┘        │ Ollama, PBX SSH, │
       │ uses only                              │ ffmpeg, STT      │
       ▼                                        └──────────────────┘
┌──────────────┐
│   domain/    │   ← inert: dataclasses, rules, config. Imports nothing internal.
└──────────────┘
```

### Layer responsibilities

| Layer | Contents | May import |
| ----- | -------- | ---------- |
| `domain/` | Inert business concepts: `models.py`, `keywords.py`, `rules.py`, `call_datetime.py`, `config.py`, `pbx.py`, `stt.py`, `ai_apply.py`, `reporting.py`, `stt_runs.py` | stdlib / third-party only |
| `ports/` | `Protocol` interfaces: `StoragePort`, `LlmPort`, `AudioPort`, `PbxPort`, `SttProcessorPort`, `KeywordSource`, `ReportingSource`, `AiApplyStorePort`, … | stdlib / third-party only |
| `core/` | Business logic & orchestration: `pipeline.py`, `planner.py`, `reporting_service.py`, `keywords_*` services, `stt_service.py`, `snapshot_export.py` | `domain` + `ports` (+ stdlib/third-party) — **never** `adapters` or `api` |
| `adapters/` | Concrete implementations: Postgres storage/reporting/keywords, JSON/YAML fallbacks, Ollama LLM, PBX SSH/Asterisk, ffmpeg audio, faster-whisper & Canary STT, migrations | `domain` + `ports` — **never** `core` or `api` |
| `api/` | FastAPI edge: `app.py`, `routes/*`, `runner.py`, `deps.py`, `job_store.py`, `schemas.py` | everything (composition root) |

Top-level entrypoints (`cli.py`, `migrate_storage.py`, `stt_replay.py`,
`stt_compare.py`) are also composition roots: they construct adapters and pass
them into core.

### Why hexagonal here

- **Core is testable in isolation** with fakes (no Postgres, no Ollama, no SSH).
- **Driver selection lives at the edge**: whether a source is Postgres or
  JSON/YAML is decided once in `api/deps.py` / entrypoints, never inside core.
- **Swappable infrastructure**: STT backends (whisper vs canary), storage
  (Postgres vs JSON), and LLM providers are all behind ports.

---

## 3. Key patterns

### 3.1 Dependency injection via constructor + factory callables

Core services receive already-constructed collaborators:

```python
class Pipeline:
    def __init__(self, config: AppConfig, storage: StoragePort, audio: AudioPort,
                 llm: LlmPort, pbx: PbxPort, stt: SttProcessorPort | None = None,
                 secondary_storage: StoragePort | None = None): ...
```

Where a collaborator must be built lazily (per request/job), the edge passes a
**zero-arg factory callable** instead of an instance — e.g.
`reporting_source_factory: Callable[[], ReportingSource | None]` in
`core/ai_apply.py`, and `keyword_source_factory()` / `reporting_source_factory()`
in `api/deps.py`. This keeps core free of driver-selection logic while avoiding
eager construction of expensive resources (DB pools) at import time.

### 3.2 Composition root (`api/deps.py`)

`api/deps.py` is the single place that:

- reads `POSTGRES_DSN` (`get_postgres_dsn()` / `require_postgres_dsn()` —
  raises HTTP 405 with a clear detail when unset);
- selects keyword/reporting drivers (Postgres vs YAML/JSON fallback) with **eager
  YAML validation** (fail loudly with HTTP 500 on invalid/missing keyword YAML);
- exposes FastAPI `Depends`-compatible factories for routes.

This replaced per-route DSN-check + adapter-construction boilerplate that was
duplicated across four route modules (antipattern C4).

### 3.3 Ports as `typing.Protocol`

All interfaces are structural (`Protocol`) rather than ABCs — adapters satisfy
them implicitly, which keeps the port layer inert and avoids inheritance
coupling. Examples: `StoragePort`, `LlmPort`, `KeywordSource`, `ReportingSource`.

### 3.4 Migration-based schema (single source of truth for DDL)

All Postgres DDL lives exclusively in `src/adapters/migrations/` (V001, V002, …)
and is applied by `apply_pending_migrations()` from
`PostgresStorage.ensure_ready()`. There is **no inline DDL** in adapter modules
(antipattern C1 — previously triple-duplicated).

### 3.5 Config as an explicit value object (`AppConfig`)

- `domain/config.py` exposes `load_app_config() -> AppConfig`, a dataclass that
  reads env/YAML **at call time**, not import time.
- No module-level env reads or path constants in the domain layer. Paths are
  lazy getters: `get_root()`, `get_calls_raw()`, `get_out()`, `get_analysis_dir()`,
  `get_keywords_config()`, etc.
- `.env` loading is explicit and idempotent: `ensure_env_loaded()` (guarded by a
  module flag) is called **only from entrypoints** (`api/app.py` lifespan,
  `cli.py`, `migrate_storage.py`, `stt_replay.py`, `stt_compare.py`) before any
  config load or logging setup. Overlay semantics: explicit process env always
  wins over `.env`; `PROJECT_ROOT` is never taken from `.env`.
- Rate limiting is config-driven: `AppConfig.ollama_rate_limit` /
  `AppConfig.ollama_rate_interval` feed the Ollama limiter (see 3.6).

### 3.6 Per-config rate limiter cache (no import-time singleton)

`adapters/llm_ollama.py` keeps `_rate_limiter_cache: dict[tuple[int, float],
_RateLimiter]` keyed by `(limit, interval)` and resolves the limiter via
`_get_rate_limiter(config)`. Consequences:

- no config captured at import time;
- different `AppConfig`s get distinct limiters;
- tests can construct isolated limiters.

The limiter enforces the interval **at acquire time** (sleeps the deficit before
granting the slot), so `release()` is non-blocking (antipattern E10).

### 3.7 Background job orchestration (`api/runner.py` + `job_store.py`)

- Jobs are async: routes enqueue work, `runner.py` executes it in background
  tasks; `job_store.py` is an in-memory registry **capped at 500 entries** with
  eviction of old finished jobs (antipattern E3).
- The runner owns post-process orchestration (keyword refresh → keyword AI
  analysis) *after* a successful process job. The pipeline itself does not know
  about these flows — processing/storage and downstream enrichment are separate
  concerns.
- Correlation IDs: set per request in `api/app.py`, propagated into background
  tasks so logs trace end-to-end (antipattern E9).

### 3.8 Fail-closed external integrations

- **PBX SSH** (`adapters/pbx_ssh.py`): defaults to paramiko `RejectPolicy`;
  `AutoAddPolicy` only with explicit `PBX_SSH_INSECURE_AUTOADD=1` opt-in
  (antipattern E5).
- **Health probe** (`api/routes/health.py`): Ollama liveness is cached with a
  short TTL and **failures are logged** (`logger.warning`) rather than silently
  swallowed (antipatterns E4/M3).
- **Logging** (`logging_config.py`): Elasticsearch send failures are logged, not
  swallowed (antipattern E6).

### 3.9 Read-side vs write-side separation

Report endpoints (`/reports/*`) are strictly read-side: they never trigger
sync/process/AI flows. Write-side flows are explicit jobs. This keeps the API
predictable and avoids accidental heavy work from a GET.

---

## 4. Design decisions & rationale

| # | Decision | Rationale |
| - | -------- | --------- |
| 1 | Postgres-first; JSON/YAML as dev/legacy fallback only | Production needs durability, concurrency, and SQL drill-downs; JSON mode kept for local debugging and migration tooling (`migrate_storage.py`). |
| 2 | Strict layering enforced by AST tests (`tests/test_layering.py`) | Prevents regression of the exact antipatterns fixed in q38: `core`/`adapters` importing each other or `api`, and env reads outside sanctioned modules. |
| 3 | Env hygiene allowlist (only `domain/config.py`, entrypoints, `logging_config.py` may read env) | Centralizes configuration; makes import-time side effects impossible in domain/core/adapters. |
| 4 | No import-time side effects in `domain/config.py`; explicit `ensure_env_loaded()` at entrypoints | Importing the domain layer must be inert; tests that change env after import no longer see stale frozen constants (antipattern B2). |
| 5 | Driver selection centralized in `api/deps.py` with eager YAML validation | One place owns "Postgres if DSN set, else YAML/JSON"; invalid keyword config fails fast with a clear HTTP 500 instead of surfacing deep in a request. |
| 6 | Factory callables injected into core for lazy collaborators | Core stays free of construction logic; expensive resources (DB pools) are created only when actually needed. |
| 7 | DDL only in `adapters/migrations/` | Single source of truth for schema; eliminates triple-duplicated inline DDL and the removed `stt_runs_schema.py`. |
| 8 | `AppConfig` value object + per-config limiter cache | Config is explicit, testable, and reconfigurable; no process-wide singletons capturing env at import (antipatterns B3/M2/M4). |
| 9 | Pipeline receives optional `secondary_storage: StoragePort` instead of constructing `PostgresStorage` itself | Removes core→adapter import and the isinstance capability check; the edge decides whether a secondary Postgres sync target exists (antipattern A1). |
| 10 | AI-apply schemas as plain domain dataclasses (`domain/ai_apply.py`) + `AiApplyStorePort` | Core must not depend on FastAPI/pydantic; pydantic models stay at the API boundary and are converted there (antipattern A2). |
| 11 | Shared SQL constants & shared helpers (`_utc_now_iso`, `_parse_int_env`, `_jsonb`, filter builders) extracted | Removes cross-adapter duplication (antipatterns C2, C3, C5). |
| 12 | Job store capped + eviction | Prevents unbounded memory growth in long-running API processes (antipattern E3). |
| 13 | Fail-closed SSH host-key policy; logged (not swallowed) external failures | Security default for PBX access; observability for Ollama/ES outages. |
| 14 | Correlation IDs propagated into background tasks | End-to-end request tracing across async job execution (antipattern E9). |
| 15 | `parse_call_datetime` in `domain/call_datetime.py`, re-exported from the adapter | Domain owns the parsing rule; adapter re-export kept for backward compatibility (antipattern A1). |

---

## 5. Guardrails (how the design is kept honest)

1. **Layering test** — `tests/test_layering.py` parses every module under
   `src/` with `ast` and asserts:
   - `domain/*`, `ports/*` import only stdlib/third-party;
   - `core/*` imports only `domain`, `ports`, stdlib, third-party (no `adapters`, no `api`);
   - `adapters/*` imports only `domain`, `ports`, stdlib, third-party (no `core`, no `api`).
2. **Env hygiene check** — same test file: `os.getenv`/`os.environ` allowed only
   in the sanctioned modules (`domain/config.py`, `domain/envutil.py`,
   `api/app.py`, `api/runner.py`, `cli.py`, `logging_config.py`,
   `migrate_storage.py`, `stt_compare.py`, `stt_replay.py`).
3. **Full test suite** — `.venv/bin/python -m pytest tests/` (unit, no external
   services) plus optional `--run-integration` (testcontainers Postgres).
4. **Type checking** — `mypy --no-incremental src tests`.

Current verification state: full suite **227 passed, 91 skipped**; layering
guardrails **4 passed**.

---

## 6. Where things live (quick map)

- **Entry / composition:** `src/api/app.py` (FastAPI lifespan), `src/api/runner.py`,
  `src/api/deps.py`, `src/cli.py`, `src/migrate_storage.py`, `src/stt_replay.py`,
  `src/stt_compare.py`.
- **Config:** `src/domain/config.py` (`AppConfig`, `load_app_config()`,
  `ensure_env_loaded()`, path getters).
- **Pipeline & services:** `src/core/pipeline.py`, `src/core/planner.py`,
  `src/core/reporting_service.py`, `src/core/keywords_*.py`,
  `src/core/stt_service.py`, `src/core/snapshot_export.py`.
- **Interfaces:** `src/ports/*.py`.
- **Infrastructure:** `src/adapters/*` (Postgres, JSON/YAML, Ollama, PBX SSH,
  ffmpeg, STT backends, `migrations/`).
- **API surface:** `src/api/routes/{health,jobs,reports,keywords,keywords_ai,keywords_generation,managers}.py`,
  `src/api/schemas.py`, `src/api/job_store.py`.
- **Guardrails:** `tests/test_layering.py`; issue history in `DEVPLAN_ANTIPATTERNS_q38.md`
  and `DEVPLAN_ANTIPATTERNS_q38_FIX_PLAN.md`.
