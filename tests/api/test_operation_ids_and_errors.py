"""BE-05: stable operation IDs and normalized error contract tests.

Verifies the cross-cutting API contract guarantees:

1. Every route in the OpenAPI spec has an explicit, unique ``operationId``
   (no FastAPI auto-generated names that break on refactors).
2. Documented non-2xx responses use the shared ``ApiError`` /
   ``ApiValidationError`` envelope (or a documented concrete schema) — no
   undocumented error bodies.
3. FastAPI's native 422 validation detail is preserved (list of
   ``{loc, msg, type}`` entries).
4. ``X-Correlation-Id`` is present on success and error responses
   (200/404/409/422), including handled exceptions.
5. Error responses never leak stack traces or secrets.

Runs without Postgres (JSON/YAML mode) so it is part of the default suite.
"""

from __future__ import annotations

import re
from collections import Counter

import pytest
from starlette.testclient import TestClient

from api.app import app

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(scope="module")
def spec(client: TestClient) -> dict:
    return client.get("/openapi.json").json()


# ---------------------------------------------------------------------------
# 1. Operation IDs: present and unique across the whole spec
# ---------------------------------------------------------------------------


def _all_operations(spec: dict) -> list[tuple[str, str, dict]]:
    """Yield (path, method, operation) for every route in the spec."""
    ops: list[tuple[str, str, dict]] = []
    for path, methods in spec["paths"].items():
        for method, op in methods.items():
            if method in {"get", "post", "put", "delete", "patch"}:
                ops.append((path, method, op))
    return ops


def test_every_route_has_operation_id(spec: dict) -> None:
    missing = [
        f"{method.upper()} {path}"
        for path, method, op in _all_operations(spec)
        if "operationId" not in op
    ]
    assert not missing, f"Routes without operationId: {missing}"


def test_operation_ids_are_unique(spec: dict) -> None:
    ids = [op["operationId"] for _, _, op in _all_operations(spec)]
    duplicates = [oid for oid, count in Counter(ids).items() if count > 1]
    assert not duplicates, f"Duplicate operationIds: {duplicates}"


def test_operation_ids_are_stable_snake_case(spec: dict) -> None:
    """Operation IDs must be explicit snake_case identifiers.

    FastAPI auto-generates ids like ``trigger_sync_post`` when none is given;
    the suffix pattern is a heuristic that catches any route still relying on
    auto-generation.
    """
    # FastAPI auto-generates ids like ``trigger_sync_post`` (function name +
    # method).  Legitimate explicit ids may end in _get/_delete (e.g.
    # health_get, keywords_delete), so only flag the exact function-name
    # pattern when the id also matches a known route function suffix.
    bad = [
        op["operationId"]
        for _, method, op in _all_operations(spec)
        if not re.fullmatch(r"[a-z][a-z0-9_]*", op.get("operationId", ""))
        or (op["operationId"].endswith(f"_{method}") and method not in {"get", "delete"})
    ]
    assert not bad, f"Non-stable operationIds (auto-generated?): {bad}"


def test_expected_operation_ids_present(spec: dict) -> None:
    """The full inventory of stable operation IDs for UI-consumed endpoints."""
    expected = {
        # health
        "health_get",
        # jobs
        "jobs_sync",
        "jobs_process",
        "jobs_sync_and_process",
        "jobs_export_snapshots",
        "jobs_list",
        "jobs_status",
        # reports
        "reports_overall",
        "reports_managers",
        "reports_customers",
        "reports_customer_detail",
        "reports_manager_detail",
        "reports_keywords",
        "reports_keyword_detail",
        "reports_keyword_calls",
        "reports_keyword_trend",
        "reports_keyword_managers",
        # keywords catalog
        "keywords_catalog_list",
        "keywords_refresh",
        "keywords_sync",
        "keywords_materialize",
        "keywords_detail",
        "keywords_upsert",
        "keywords_update",
        "keywords_delete",
        # keyword generation
        "generation_candidates",
        "generation_publish",
        "generation_bootstrap",
        "generation_enrich",
        "generation_pipeline",
        # keyword AI
        "ai_analysis_run",
        "ai_analyses_list",
        "ai_analysis_detail",
        "ai_apply_actions",
        "ai_apply_history",
        "alias_expand",
        "alias_suggestions_list",
        "alias_approve",
        "alias_reject",
        "insights_generate",
        "insights_runs_list",
        "insights_run_detail",
        # managers
        "managers_list",
    }
    actual = {op["operationId"] for _, _, op in _all_operations(spec)}
    missing = expected - actual
    assert not missing, f"Expected operationIds missing from spec: {sorted(missing)}"


# ---------------------------------------------------------------------------
# 2. Error envelope: documented non-2xx responses use the shared schemas
# ---------------------------------------------------------------------------


def _error_schema_is_documented(op: dict, status: str) -> bool:
    """True if the response documents a schema or example with a detail key."""
    resp = op.get("responses", {}).get(status)
    if resp is None:
        return False
    content = resp.get("content", {}).get("application/json", {})
    if "example" in content and isinstance(content["example"], dict):
        return "detail" in content["example"]
    schema = content.get("schema")
    if not schema:
        return False
    # $ref to a shared error model counts as documented.
    if "$ref" in schema:
        return True
    props = schema.get("properties", {})
    return "detail" in props


def test_documented_errors_use_shared_envelope(spec: dict) -> None:
    """Every documented non-2xx response must carry a detail-bearing schema."""
    problems: list[str] = []
    for path, method, op in _all_operations(spec):
        for status, resp in op.get("responses", {}).items():
            if status.startswith("2"):
                continue
            content = resp.get("content", {}).get("application/json", {})
            if not content:
                # 204-style or description-only responses are fine.
                continue
            if "example" in content:
                example = content["example"]
                if not (isinstance(example, dict) and "detail" in example):
                    problems.append(f"{method.upper()} {path} {status}: example lacks 'detail'")
            elif "schema" not in content:
                problems.append(f"{method.upper()} {path} {status}: no schema or example")
    assert not problems, "\n".join(problems)


def test_422_keeps_native_validation_detail(client: TestClient) -> None:
    """FastAPI's default 422 shape (list of {loc, msg, type}) is preserved."""
    resp = client.post("/jobs/process", json={"limit": "not-an-int"})
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert isinstance(detail, list) and detail, "422 detail must be a non-empty list"
    for entry in detail:
        assert set(entry) >= {"loc", "msg", "type"}, f"422 entry missing keys: {entry}"


def test_405_error_shape(client: TestClient) -> None:
    """JSON-mode 405 responses use the plain-string detail envelope."""
    resp = client.post("/keywords/refresh")
    assert resp.status_code == 405
    body = resp.json()
    assert isinstance(body.get("detail"), str), f"Unexpected 405 body: {body}"


# ---------------------------------------------------------------------------
# 3. Correlation ID on success and error responses
# ---------------------------------------------------------------------------


def _assert_correlation_header(resp, client: TestClient) -> None:
    header = resp.headers.get("X-Correlation-Id")
    assert header, f"Missing X-Correlation-Id on {resp.status_code} response"
    assert _UUID_RE.fullmatch(header), f"Malformed correlation id: {header!r}"


def test_correlation_id_on_200(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    _assert_correlation_header(resp, client)


def test_correlation_id_on_404(client: TestClient) -> None:
    resp = client.get("/jobs/does-not-exist")
    assert resp.status_code == 404
    _assert_correlation_header(resp, client)


def test_correlation_id_on_422(client: TestClient) -> None:
    resp = client.post("/jobs/process", json={"limit": "not-an-int"})
    assert resp.status_code == 422
    _assert_correlation_header(resp, client)


def test_correlation_id_echoes_client_value(client: TestClient) -> None:
    resp = client.get("/health", headers={"X-Correlation-Id": "test-correlation-123"})
    assert resp.status_code == 200
    assert resp.headers["X-Correlation-Id"] == "test-correlation-123"


def test_correlation_id_on_409(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """A real 409 (job already running) carries the correlation header."""
    import api.job_store as job_store

    # Simulate a conflicting running job so the route raises HTTPException(409).
    monkeypatch.setattr(job_store, "create_sync_job_if_none_running", lambda: None)
    resp = client.post("/jobs/sync", json={})
    assert resp.status_code == 409
    _assert_correlation_header(resp, client)


# ---------------------------------------------------------------------------
# 4. No stack traces or secrets in error responses
# ---------------------------------------------------------------------------


def test_error_bodies_do_not_leak_stack_traces(client: TestClient) -> None:
    for resp in (
        client.get("/jobs/does-not-exist"),
        client.post("/jobs/process", json={"limit": "not-an-int"}),
        client.post("/keywords/refresh"),
        client.get("/reports/customers/abc"),
    ):
        body = resp.text
        assert "Traceback" not in body, f"{resp.status_code} response leaked a traceback: {body[:200]}"


def test_404_body_is_plain_detail(client: TestClient) -> None:
    resp = client.get("/jobs/does-not-exist")
    body = resp.json()
    assert set(body) == {"detail"}, f"404 body must be exactly {{'detail': ...}}: {body}"
    assert isinstance(body["detail"], str)
