"""BE-02: response-model contract tests for health, managers, and core reports.

Verifies that the ``response_model`` declarations on the BE-02 routes produce
concrete OpenAPI schemas and that actual endpoint output validates against
the models (including empty datasets).  Runs without Postgres (JSON/YAML mode)
so it is part of the default test suite.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from api.app import app
from api.report_schemas import (
    CustomerFollowupReport,
    CustomersReport,
    HealthResponse,
    ManagerEntry,
    ManagerReportDetail,
    ManagersReport,
    OverallReport,
)


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# OpenAPI schema assertions
# ---------------------------------------------------------------------------


def _operation_schema(client: TestClient, path: str, method: str = "get") -> dict:
    spec = client.get("/openapi.json").json()
    op = spec["paths"][path][method]
    assert "operationId" in op, f"{method.upper()} {path} missing operationId"
    resp = op["responses"]["200"]
    content = resp.get("content", {}).get("application/json", {})
    schema = content.get("schema", {})
    assert schema, f"{method.upper()} {path} has no concrete 200 schema"
    return schema


def test_health_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/health")
    # Resolves to HealthResponse via $ref
    ref = schema.get("$ref", "")
    assert "HealthResponse" in ref or "properties" in schema


def test_managers_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/managers")
    # list[ManagerEntry] → array of ManagerEntry refs
    assert schema.get("type") == "array"
    items = schema.get("items", {})
    ref = items.get("$ref", "")
    assert "ManagerEntry" in ref


def test_overall_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/reports/overall")
    ref = schema.get("$ref", "")
    assert "OverallReport" in ref


def test_managers_report_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/reports/managers")
    ref = schema.get("$ref", "")
    assert "ManagersReport" in ref


def test_customers_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/reports/customers")
    ref = schema.get("$ref", "")
    assert "CustomersReport" in ref


def test_customer_detail_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/reports/customers/{customer_phone}")
    ref = schema.get("$ref", "")
    assert "CustomerFollowupReport" in ref


def test_manager_detail_openapi_schema(client: TestClient) -> None:
    schema = _operation_schema(client, "/reports/manager/{manager_id}")
    ref = schema.get("$ref", "")
    assert "ManagerReportDetail" in ref


# ---------------------------------------------------------------------------
# Stable operation IDs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "method", "expected_op_id"),
    [
        ("/health", "get", "health_get"),
        ("/managers", "get", "managers_list"),
        ("/reports/overall", "get", "reports_overall"),
        ("/reports/managers", "get", "reports_managers"),
        ("/reports/customers", "get", "reports_customers"),
        ("/reports/customers/{customer_phone}", "get", "reports_customer_detail"),
        ("/reports/manager/{manager_id}", "get", "reports_manager_detail"),
    ],
)
def test_stable_operation_ids(
    client: TestClient, path: str, method: str, expected_op_id: str
) -> None:
    spec = client.get("/openapi.json").json()
    op = spec["paths"][path][method]
    assert op["operationId"] == expected_op_id


# ---------------------------------------------------------------------------
# Endpoint output validates against models (real payloads)
# ---------------------------------------------------------------------------


def test_health_output_matches_model(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    parsed = HealthResponse.model_validate(r.json())
    assert parsed.status == "ok"
    assert parsed.ollama in {"up", "down"}
    assert parsed.ollama_url


def test_managers_output_matches_model(client: TestClient) -> None:
    r = client.get("/managers")
    assert r.status_code == 200
    entries = [ManagerEntry.model_validate(m) for m in r.json()]
    for entry in entries:
        assert entry.id
        assert entry.name
        assert entry.role in {"management", "sales"}


def test_overall_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/overall")
    assert r.status_code == 200
    report = OverallReport.model_validate(r.json())
    assert report.total_calls >= 0
    assert report.data_source in {"postgres", "json", "yaml"}
    # top_* are [label, count] pairs
    for label, count in report.top_intents:
        assert isinstance(label, str)
        assert isinstance(count, int)


def test_managers_report_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/managers")
    assert r.status_code == 200
    report = ManagersReport.model_validate(r.json())
    assert report.total_managers == len(report.all_managers)
    for mgr in report.all_managers:
        assert mgr.manager_id
        assert mgr.total_calls >= 0


def test_customers_report_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/customers")
    assert r.status_code == 200
    report = CustomersReport.model_validate(r.json())
    assert report.total_customers == len(report.all_customers)
    for cust in report.all_customers:
        assert cust.customer_phone


def test_customer_detail_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/customers")
    assert r.status_code == 200
    customers = r.json().get("all_customers", [])
    if not customers:
        pytest.skip("No customers in dataset")
    phone = customers[0]["customer_phone"]
    r2 = client.get(f"/reports/customers/{phone}")
    assert r2.status_code == 200
    detail = CustomerFollowupReport.model_validate(r2.json())
    assert detail.customer_phone == phone
    for call in detail.calls:
        assert call.call_id
        assert 0.0 <= call.spam_probability <= 1.0


def test_manager_detail_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/managers")
    assert r.status_code == 200
    managers = r.json().get("all_managers", [])
    if not managers:
        pytest.skip("No managers in dataset")
    mid = managers[0]["manager_id"]
    r2 = client.get(f"/reports/manager/{mid}")
    assert r2.status_code == 200
    detail = ManagerReportDetail.model_validate(r2.json())
    assert detail.manager_id == mid


# ---------------------------------------------------------------------------
# Empty-dataset behavior (models must accept zero-row payloads)
# ---------------------------------------------------------------------------


def test_overall_model_accepts_empty_dataset() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "data_source": "json",
        "filters": {
            "date_from": None,
            "date_to": None,
            "manager_id": None,
            "role": None,
            "direction": None,
            "intent": None,
            "outcome": None,
            "spam_only": False,
            "effective_only": False,
        },
        "total_calls": 0,
        "analyzed_calls": 0,
        "unique_managers": 0,
        "skipped_metrics_available": False,
        "spam_calls": 0,
        "effective_calls": 0,
        "total_duration_seconds": 0.0,
        "top_intents": [],
        "top_outcomes": [],
        "top_questions": [],
    }
    report = OverallReport.model_validate(payload)
    assert report.total_calls == 0
    assert report.top_intents == []


def test_managers_report_model_accepts_empty_dataset() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "data_source": "json",
        "filters": {
            "date_from": None,
            "date_to": None,
            "manager_id": None,
            "role": None,
            "direction": None,
            "intent": None,
            "outcome": None,
            "spam_only": False,
            "effective_only": False,
        },
        "skipped_metrics_available": False,
        "role_summary": {},
        "by_role": {},
        "all_managers": [],
        "total_managers": 0,
    }
    report = ManagersReport.model_validate(payload)
    assert report.all_managers == []


def test_customers_report_model_accepts_empty_dataset() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "data_source": "json",
        "filters": {
            "date_from": None,
            "date_to": None,
            "manager_id": None,
            "role": None,
            "direction": None,
            "intent": None,
            "outcome": None,
            "spam_only": False,
            "effective_only": False,
        },
        "skipped_metrics_available": False,
        "all_customers": [],
        "total_customers": 0,
    }
    report = CustomersReport.model_validate(payload)
    assert report.all_customers == []


def test_freshness_and_ai_analysis_absent_in_json_mode(client: TestClient) -> None:
    """In JSON/YAML mode freshness is absent and keyword_ai_analysis is null."""
    r = client.get("/reports/overall")
    assert r.status_code == 200
    body = r.json()
    # No POSTGRES_DSN → no freshness key
    assert "freshness" not in body or body["freshness"] is None
    # keyword_ai_analysis present but null (no PG analysis store)
    assert body.get("keyword_ai_analysis") is None
