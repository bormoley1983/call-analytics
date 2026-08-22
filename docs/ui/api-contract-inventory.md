# API Contract Inventory (DISC-01)

Status: verified baseline  
Date: 2026-08-21  
Source of truth: `src/api/app.py` + `src/api/routes/*.py` + `src/api/schemas.py`,
inspected read-only. No production code was changed for this task.

This document is the handoff used by all `BE-*` and `FE-*` tasks in
`DEVPLAN_UI_IMPLEMENTATION.md`.

## 1. Verified HTTP topology

### Development

- Backend: `uvicorn api.app:app --host 0.0.0.0 --port 8000` (Compose service
  `stt-whisper`, or locally via `.venv`). Host port mapping: `8800 -> 8000`.
- The API is served directly at the origin root (`/health`, `/jobs`, …). There is
  **no** `/api` prefix on the backend today.
- Ollama is probed from the container via `http://host.docker.internal:11434`
  (`OLLAMA_URL` env, `extra_hosts: host-gateway`).
- Data source selection (single source of truth in `src/api/deps.py`):
  - `POSTGRES_DSN` set -> Postgres adapters.
  - `POSTGRES_DSN` missing -> JSON/YAML file adapters (legacy/dev mode).

### Production (planned, not yet built)

- nginx serves the static SPA and reverse-proxies `/api/` to FastAPI (OPS-01).
- The UI will see the API base as same-origin `/api`; the backend keeps serving at
  its own root. The Vite dev proxy (`UI-01`) maps `/api -> http://localhost:8800`.

### Correlation IDs

- `src/api/app.py` middleware sets/propagates `X-Correlation-Id` on **all**
  responses, including errors (middleware wraps `call_next` and adds the header
  after the response is produced). Verified present on success and error paths.

## 2. Endpoint matrix

Legend — risk: R = read-only, M = mutation (catalog/data), J = job trigger
(background work), A = AI/LLM-backed (slow, external Ollama dependency).

| # | Method / Path | Request schema | Success model | Declared errors | Filters / sort / pagination | Data source | Risk |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `GET /health` | — | **none** (dict) | — | — | Ollama probe (5 s TTL cache) | R |
| 2 | `POST /jobs/sync` | `SyncRequest` | `JobResponse` (202) | 409, 422 | — | in-memory job store | J |
| 3 | `POST /jobs/process` | `ProcessRequest` | `JobResponse` (202) | 409, 422 | — | in-memory job store | J |
| 4 | `POST /jobs/sync-and-process` | `ProcessRequest` | `JobResponse` (202) | 409, 422 | — | in-memory job store | J |
| 5 | `POST /jobs/export-snapshots` | — | `JobResponse` (202) | 409 | — | in-memory job store | J |
| 6 | `GET /jobs` | — | `list[JobResponse]` | — | none (capped at 50, newest first) | in-memory job store | R |
| 7 | `GET /jobs/{job_id}` | — | `JobResponse` | 404 | — | in-memory job store | R |
| 8 | `GET /reports/overall` | `ReportFiltersQuery` | **none** (dict) | 422 | filters only | PG or JSON/YAML | R |
| 9 | `GET /reports/managers` | `ReportFiltersQuery` + `ManagersSortQuery` | **none** (dict) | 422 | filters + sort, **no pagination** | PG or JSON/YAML | R |
| 10 | `GET /reports/customers` | `ReportFiltersQuery` + `CustomersSortQuery` | **none** (dict) | 422 | filters + sort, **no pagination** | PG or JSON/YAML | R |
| 11 | `GET /reports/customers/{customer_phone}` | `ReportFiltersQuery` | **none** (dict) | 400, 404, 422 | filters only | PG or JSON/YAML | R |
| 12 | `GET /reports/manager/{manager_id}` | `ReportFiltersQuery` | **none** (dict) | 400, 404, 422 | filters only (path id wins) | PG or JSON/YAML | R |
| 13 | `GET /reports/keywords` | `ReportFiltersQuery` + `KeywordsSortQuery` | **none** (dict) | 422 | filters + sort, **no pagination** | PG (materialized) or dynamic | R |
| 14 | `GET /reports/keywords/{keyword_id}` | `ReportFiltersQuery` | **none** (dict) | 400, 404, 422 | filters only | PG (materialized) or dynamic | R |
| 15 | `GET /reports/keywords/{keyword_id}/calls` | `ReportFiltersQuery` + `PaginationQuery` + `KeywordCallsSortQuery` | **none** (dict) | 400, 404, 405, 409, 422 | filters + sort + **offset/limit pagination** (`limit=50`, `offset=0`) | PG only, materialized required | R |
| 16 | `GET /reports/keywords/{keyword_id}/trend` | `ReportFiltersQuery` | **none** (dict) | 400, 404, 405, 409, 422 | filters only | PG only, materialized required | R |
| 17 | `GET /reports/keywords/{keyword_id}/managers` | `ReportFiltersQuery` + `KeywordManagersSortQuery` | **none** (dict) | 400, 404, 405, 409, 422 | filters + sort, no pagination | PG only, materialized required | R |
| 18 | `GET /keywords` | — | **none** (dict) | — | none | PG or YAML | R |
| 19 | `POST /keywords/refresh` | `KeywordSyncRequest \| None` | **none** (dict, may attach `keyword_ai_analysis`) | 400, 405, 422 | — | PG only | M+A |
| 20 | `POST /keywords/sync` | `KeywordSyncRequest` | **none** (dict) | 400, 405, 422 | — | PG only (admin/debug) | M+A |
| 21 | `POST /keywords/materialize` | — | **none** (dict) | 405 | — | PG only (admin/debug) | M+A |
| 22 | `GET /keywords/{keyword_id}` | — | **none** (dict) | 400, 404, 422 | — | PG or YAML | R |
| 23 | `PUT /keywords/{keyword_id}` | `KeywordUpsertRequest` | **none** (dict) | 400, 404, 405, 422 | — | PG only | M |
| 24 | `DELETE /keywords/{keyword_id}` | — | 204 no content | 400, 404, 405, 422 | — | PG only | M |
| 25 | `POST /keywords/upsert` | `KeywordUpsertRequest` | **none** (dict) | 405, 422 | — | PG only | M |
| 26 | `POST /keywords/catalog/analysis` | `KeywordCatalogAnalysisRequest` | **none** (dict) | 502, 422 | — | PG or YAML + Ollama; persists to PG when DSN set | A |
| 27 | `GET /keywords/catalog/analyses` | `limit` query (1..500, default 50) | **none** (dict `{returned, analyses}`) | 405, 422 | limit only | PG only | R |
| 28 | `GET /keywords/catalog/analyses/{analysis_id}` | — | **none** (dict) | 404, 405, 422 | — | PG only | R |
| 29 | `POST /keywords/catalog/analyses/{analysis_id}/apply` | `AIApplyRequest` | `AIApplyResult` (201) | 422 (404 raised at runtime, undeclared) | — | PG only | M+A (highest risk) |
| 30 | `GET /keywords/catalog/analyses/{analysis_id}/apply/history` | `limit`, `offset` query | `list[AIApplyHistoryEntry]` | 422 | limit/offset (no total count) | PG only | R |
| 31 | `POST /keywords/catalog/{keyword_id}/expand-aliases` | `KeywordAliasExpandRequest \| None` | `KeywordAliasExpandResult` | 422 (404/500 raised at runtime, undeclared) | — | PG or YAML + Ollama; persists pending suggestion to PG | A |
| 32 | `GET /keywords/catalog/aliases/suggestions` | `keyword_id`, `status`, `limit` query | `list[AliasSuggestionEntry]` | 422 (405 raised at runtime, undeclared) | filters + limit (no offset) | PG only | R |
| 33 | `POST /keywords/catalog/aliases/suggestions/{suggestion_id}/approve` | — | **none** (dict from store) | 422 (405 at runtime, undeclared) | — | PG only | M |
| 34 | `POST /keywords/catalog/aliases/suggestions/{suggestion_id}/reject` | — | **none** (dict from store) | 422 (405 at runtime, undeclared) | — | PG only | M |
| 35 | `POST /keywords/catalog/insights/deep/generate` | `DeepInsightRequest` | `DeepInsightResult` | 422 (500 raised at runtime, undeclared) | — | PG or YAML + Ollama; persists run to PG when DSN set | A |
| 36 | `GET /keywords/catalog/insights/deep/runs` | `limit`, `insight_type_filter` query | **none** (dict from store) | 422 (405 at runtime, undeclared) | limit + type filter | PG only | R |
| 37 | `GET /keywords/catalog/insights/deep/runs/{run_id}` | — | **none** (dict from store) | 422 (404/405 at runtime, undeclared) | — | PG only | R |
| 38 | `POST /keywords/generation/candidates` | `KeywordGenerationRequest` | **none** (dict + `filters`) | 405, 422 | filters in body | PG only | A (read-only output) |
| 39 | `POST /keywords/generation/publish` | `KeywordGenerationPublishRequest` | **none** (dict `{publish, materialized[, materialize]}`) | 405, 422 | — | PG only | M+A |
| 40 | `POST /keywords/generation/bootstrap` | `KeywordGenerationBootstrapRequest` | **untyped object** (`dict[str, object]`) | 405, 422 | filters in body | PG only | M+A |
| 41 | `POST /keywords/generation/enrich` | `KeywordGenerationEnrichRequest` | **none** (dict) | 422 (500 at runtime, undeclared) | — | Ollama only (no DB) | A |
| 42 | `POST /keywords/generation/pipeline` | `KeywordGenerationPipelineRequest` | **none** (dict) | 405, 422 | filters in body | PG only | M+A |
| 43 | `GET /managers` | — | **none** (list of dicts) | — | none | config (`managers.yaml`) | R |

Total: **43 API endpoints** (+ 4 framework routes: `/openapi.json`, `/docs`,
`/docs/oauth2-redirect`, `/redoc`). `len(app.routes)` at import time is 11 because
routers are included lazily; the OpenAPI spec lists all 47 paths.

### Pagination distinction (acceptance criterion)

- **Paginated**: only `GET /reports/keywords/{keyword_id}/calls` uses true
  offset pagination (`limit` 1..500 default 50, `offset` >= 0 default 0). The
  response envelope includes pagination metadata (see BE-03 for exact shape).
- **Limit-only (not real pagination)**: `/keywords/catalog/analyses` (`limit`),
  `/keywords/catalog/aliases/suggestions` (`limit`, no offset),
  `/keywords/catalog/insights/deep/runs` (`limit`).
- **Offset+limit without total**: `/keywords/catalog/analyses/{id}/apply/history`.
- **Non-paginated aggregate reports**: all other `/reports/*` endpoints return the
  full collection. The UI must not present a paginator for them (FE-04/FE-05).

## 3. Endpoints with missing / weak response schemas

Endpoints whose 2xx OpenAPI schema is empty or an untyped object (BE-02..BE-04
will fix these):

| Endpoint | Current state | Notes |
| --- | --- | --- |
| `GET /health` | no schema | simple dict: `status`, `ollama`, `ollama_url` |
| all `/reports/*` (10 endpoints) | no schema | dicts from core services + optional `freshness` + optional `keyword_ai_analysis` attachments |
| `GET /keywords`, `GET /keywords/{id}` | no schema | catalog list/detail dicts |
| `POST /keywords/refresh`, `/sync`, `/materialize` | no schema | result dicts, may attach `keyword_ai_analysis` or `keyword_ai_analysis_error` |
| `PUT /keywords/{id}`, `POST /keywords/upsert` | no schema | keyword definition dict (6 fields) |
| `GET /managers` | no schema | list of manager dicts (5 fields) |
| `POST /keywords/catalog/analysis`, `GET .../analyses`, `GET .../analyses/{id}` | no schema | analysis run dicts |
| `POST .../expand-aliases` | typed (`KeywordAliasExpandResult`) | OK |
| `GET .../aliases/suggestions` | typed (`list[AliasSuggestionEntry]`) | OK |
| `POST .../approve`, `.../reject` | no schema | store result dicts |
| `POST .../insights/deep/generate` | typed (`DeepInsightResult`) | OK |
| `GET .../insights/deep/runs`, `.../runs/{run_id}` | no schema | store dicts |
| `POST /keywords/generation/candidates`, `/publish`, `/enrich`, `/pipeline` | no schema | pipeline result dicts |
| `POST /keywords/generation/bootstrap` | untyped object | `dict[str, object]` return annotation |

Already typed (no work needed in BE-02..BE-04 beyond reuse):
`JobResponse`, `list[JobResponse]`, `AIApplyResult`, `list[AIApplyHistoryEntry]`,
`KeywordAliasExpandResult`, `list[AliasSuggestionEntry]`, `DeepInsightResult`.

### Error-schema gaps (for BE-05)

- FastAPI default 422 validation errors are used everywhere (useful `detail` list;
  keep or explicitly adapt).
- Runtime-raised errors that are **not declared** in OpenAPI `responses`:
  - 404 on `POST .../apply` (analysis not found)
  - 404/500 on `POST .../expand-aliases`
  - 405 on alias suggestion list/approve/reject, deep-insights runs/run detail
    (raised by `require_postgres_dsn`)
  - 500 on `.../enrich`, `.../insights/deep/generate`
- Error bodies are FastAPI's `{"detail": ...}` shape; no common envelope yet.

## 4. Job types -> likely cache invalidations (for FE-02)

| Job type | Trigger endpoint | On success, invalidate |
| --- | --- | --- |
| `sync` | `POST /jobs/sync` | raw-call-dependent reports: `/reports/*`, keyword reports (data may have changed) |
| `process` | `POST /jobs/process` | all `/reports/*`; with PG: keyword catalog + materialized data auto-refreshed, so also `/keywords*`, keyword AI analysis freshness |
| `sync-and-process` | `POST /jobs/sync-and-process` | union of both above |
| `export-snapshots` | `POST /jobs/export-snapshots` | none (read-only w.r.t. reports; snapshot files only) |

Job conflict rules (from `src/api/job_store.py`, 409 semantics):

- `sync` conflicts with active `sync`, `sync-and-process`.
- `process`/`export-snapshots` conflict with active `process`, `sync-and-process`.
- `sync-and-process` conflicts with any active job type.

## 5. Known limitations (recorded, not fixed here)

1. **In-memory job store** — jobs live in process memory (`_jobs` dict, capped at
   500, finished jobs evicted). On API restart all job state is lost; a previously
   active job's `GET /jobs/{id}` returns 404. The UI must treat that as "status
   unavailable", not "failed" (FE-02/FE-03).
2. **No authentication** — no auth contract exists on the API. No auth UI may be
   built until an enforceable backend contract exists (global rule). The system is
   for localhost/trusted-network use only.
3. **Dual data-source mode** — JSON/YAML mode is legacy/dev-only; Postgres is the
   primary mode. Several endpoints are PG-only and return 405 without `POSTGRES_DSN`.
4. **AI endpoints depend on Ollama** — slow, external; failures surface as 502/500
   with string details. No retry/idempotency guarantees.
5. **No CORS configuration** — same-origin topology (nginx proxy / Vite dev proxy)
   is the intended solution; do not add CORS headers as a workaround.

## 6. Fixed stack and non-goals (from DEVPLAN_UI_IMPLEMENTATION.md §2)

Fixed: React 19 + strict TS + Vite SPA, React Router, TanStack Query, MUI (+ MUI X
Data Grid **Community only**), ECharts, React Hook Form + Zod, OpenAPI Generator
`typescript-fetch` behind an app adapter, Vitest + RTL, MSW, Playwright, nginx
static + same-origin `/api` proxy.

Non-goals (deferred): Next.js/SSR, Redux/Zustand, shadcn, TanStack Table, GraphQL,
WebSockets/SSE, microfrontends, Nx, offline writes, paid MUI X features.

## 7. Verification evidence

```bash
$ PYTHONPATH=src ./.venv/bin/python -c "from api.app import app; print(len(app.routes))"
11   # routers included lazily; OpenAPI spec lists all 47 paths (43 API + 4 framework)

$ pytest tests/api/test_job_store.py
3 passed
```

No production code was changed for DISC-01.
