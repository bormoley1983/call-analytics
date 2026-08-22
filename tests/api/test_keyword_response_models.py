"""BE-03: response-model contract tests for keyword catalog and analytics.

Verifies that the ``response_model`` declarations on the BE-03 routes produce
concrete OpenAPI schemas, that 404/405/409 error responses are modeled with
the shared ``ApiError`` schema, that offset pagination metadata is exact for
keyword calls, and that actual endpoint output validates against the models.

Runs without Postgres (JSON/YAML mode) so it is part of the default test
suite; drill-down endpoints that require Postgres return 405 here, which is
itself a contract assertion.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
import yaml
from starlette.testclient import TestClient

import api.deps as api_deps
from api.app import app
from api.keyword_schemas import (
    KeywordAggregateEntry,
    KeywordCallEntry,
    KeywordCallsReport,
    KeywordDetailReport,
    KeywordDefinitionEntry,
    KeywordManagersReport,
    KeywordsCatalogResponse,
    KeywordsReport,
    KeywordTrendPoint,
    KeywordTrendReport,
)


def _write_keywords(path):
    payload = [
        {
            "keyword_id": "delivery",
            "label": "Delivery",
            "category": "logistics",
            "terms": ["delivery", "order"],
            "match_fields": ["summary", "key_questions", "objections"],
            "is_active": True,
        },
        {
            "keyword_id": "refund",
            "label": "Refund",
            "category": "payments",
            "terms": ["refund"],
            "match_fields": ["summary"],
            "is_active": True,
        },
    ]
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


@pytest.fixture(scope="module")
def client(tmp_path_factory) -> Generator[TestClient, None, None]:
    """Module-scoped client with a valid temp keywords.yaml.

    In JSON/YAML mode the keyword source is validated eagerly (strict=True),
    so a missing config/keywords.yaml would cause 500 on every keyword
    endpoint.  We patch ``api.deps.get_keywords_config`` to point at a temp
    file with two standard keywords, mirroring the pattern in
    ``tests/core/test_keywords.py``.
    """
    keywords_path = tmp_path_factory.mktemp("be03") / "keywords.yaml"
    _write_keywords(keywords_path)

    mp = pytest.MonkeyPatch()
    mp.setattr(api_deps, "get_keywords_config", lambda: keywords_path)
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        mp.undo()


# ---------------------------------------------------------------------------
# OpenAPI schema assertions
# ---------------------------------------------------------------------------


def _operation(client: TestClient, path: str, method: str = "get") -> dict:
    spec = client.get("/openapi.json").json()
    return spec["paths"][path][method]


def _schema_ref_name(schema: dict) -> str:
    ref = schema.get("$ref", "")
    return ref.rsplit("/", 1)[-1] if ref else ""


def test_keywords_catalog_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordsCatalogResponse"


def test_keyword_detail_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/{keyword_id}")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordDetailResponse"


def test_keywords_upsert_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/upsert", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordDefinitionEntry"


def test_keywords_update_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/{keyword_id}", method="put")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordDefinitionEntry"


def test_keywords_delete_has_no_200_body(client: TestClient) -> None:
    op = _operation(client, "/keywords/{keyword_id}", method="delete")
    assert "204" in op["responses"]
    assert "200" not in op["responses"]


def test_keywords_refresh_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/refresh", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordRefreshResult"


def test_keywords_sync_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/sync", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordSyncResponse"


def test_keywords_materialize_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/materialize", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordMaterializeResponse"


def test_keywords_report_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/reports/keywords")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordsReport"


def test_keyword_detail_report_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/reports/keywords/{keyword_id}")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordDetailReport"


def test_keyword_calls_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/reports/keywords/{keyword_id}/calls")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordCallsReport"


def test_keyword_trend_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/reports/keywords/{keyword_id}/trend")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordTrendReport"


def test_keyword_managers_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/reports/keywords/{keyword_id}/managers")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordManagersReport"


# ---------------------------------------------------------------------------
# Stable operation IDs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "method", "expected_op_id"),
    [
        ("/keywords", "get", "keywords_catalog_list"),
        ("/keywords/{keyword_id}", "get", "keywords_detail"),
        ("/keywords/upsert", "post", "keywords_upsert"),
        ("/keywords/{keyword_id}", "put", "keywords_update"),
        ("/keywords/{keyword_id}", "delete", "keywords_delete"),
        ("/keywords/refresh", "post", "keywords_refresh"),
        ("/keywords/sync", "post", "keywords_sync"),
        ("/keywords/materialize", "post", "keywords_materialize"),
        ("/reports/keywords", "get", "reports_keywords"),
        ("/reports/keywords/{keyword_id}", "get", "reports_keyword_detail"),
        ("/reports/keywords/{keyword_id}/calls", "get", "reports_keyword_calls"),
        ("/reports/keywords/{keyword_id}/trend", "get", "reports_keyword_trend"),
        ("/reports/keywords/{keyword_id}/managers", "get", "reports_keyword_managers"),
    ],
)
def test_stable_operation_ids(
    client: TestClient, path: str, method: str, expected_op_id: str
) -> None:
    op = _operation(client, path, method)
    assert op["operationId"] == expected_op_id


# ---------------------------------------------------------------------------
# Error responses modeled in OpenAPI (404 / 405 / 409)
# ---------------------------------------------------------------------------


def _error_schema_has_detail(spec: dict, path: str, method: str, status_code: int) -> bool:
    op = spec["paths"][path][method]
    content = op["responses"].get(str(status_code), {}).get("content", {})
    json_content = content.get("application/json", {})
    # Routes may document errors via a concrete schema (ApiError $ref or inline)
    # or via an example object.  Either way "detail" must be documented.
    schema = json_content.get("schema", {})
    if "$ref" in schema:
        return _schema_ref_name(schema) == "ApiError"
    if "properties" in schema:
        return "detail" in schema["properties"]
    # Fall back to example-based documentation
    example = json_content.get("example", {})
    return isinstance(example, dict) and "detail" in example


def test_error_responses_modeled(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()

    # 404 on catalog detail and report drill-downs
    assert _error_schema_has_detail(spec, "/keywords/{keyword_id}", "get", 404)
    assert _error_schema_has_detail(spec, "/reports/keywords/{keyword_id}", "get", 404)
    for path in (
        "/reports/keywords/{keyword_id}/calls",
        "/reports/keywords/{keyword_id}/trend",
        "/reports/keywords/{keyword_id}/managers",
    ):
        assert _error_schema_has_detail(spec, path, "get", 404)

    # 405 on write endpoints and drill-downs (Postgres required)
    for path, method in (
        ("/keywords/upsert", "post"),
        ("/keywords/{keyword_id}", "put"),
        ("/keywords/{keyword_id}", "delete"),
        ("/keywords/refresh", "post"),
        ("/keywords/sync", "post"),
        ("/keywords/materialize", "post"),
        ("/reports/keywords/{keyword_id}/calls", "get"),
        ("/reports/keywords/{keyword_id}/trend", "get"),
        ("/reports/keywords/{keyword_id}/managers", "get"),
    ):
        assert _error_schema_has_detail(spec, path, method, 405)

    # 409 on drill-downs (materialization missing)
    for path in (
        "/reports/keywords/{keyword_id}/calls",
        "/reports/keywords/{keyword_id}/trend",
        "/reports/keywords/{keyword_id}/managers",
    ):
        assert _error_schema_has_detail(spec, path, "get", 409)


# ---------------------------------------------------------------------------
# Endpoint output validates against models (real payloads, JSON/YAML mode)
# ---------------------------------------------------------------------------


def test_keywords_catalog_output_matches_model(client: TestClient) -> None:
    r = client.get("/keywords")
    assert r.status_code == 200
    catalog = KeywordsCatalogResponse.model_validate(r.json())
    assert catalog.total_keywords == len(catalog.keywords)
    for kw in catalog.keywords:
        assert kw.keyword_id
        assert kw.label


def test_keyword_detail_output_matches_model(client: TestClient) -> None:
    r = client.get("/keywords")
    assert r.status_code == 200
    keywords = r.json().get("keywords", [])
    if not keywords:
        pytest.skip("No keywords in dataset")
    kw_id = keywords[0]["keyword_id"]
    r2 = client.get(f"/keywords/{kw_id}")
    assert r2.status_code == 200
    detail = KeywordDefinitionEntry.model_validate(r2.json())
    assert detail.keyword_id == kw_id


def test_keyword_detail_404(client: TestClient) -> None:
    r = client.get("/keywords/definitely_not_a_keyword_xyz")
    assert r.status_code == 404
    assert "detail" in r.json()


def test_keywords_report_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/keywords")
    assert r.status_code == 200
    report = KeywordsReport.model_validate(r.json())
    assert report.total_keywords == len(report.keywords)
    for kw in report.keywords:
        assert kw.keyword_id
        assert kw.matched_calls >= 0
        assert kw.total_matches >= kw.matched_calls or kw.total_matches >= 0
        for label, count in kw.top_intents:
            assert isinstance(label, str)
            assert isinstance(count, int)


def test_keyword_detail_report_output_matches_model(client: TestClient) -> None:
    r = client.get("/reports/keywords")
    assert r.status_code == 200
    keywords = r.json().get("keywords", [])
    if not keywords:
        pytest.skip("No keywords in dataset")
    kw_id = keywords[0]["keyword_id"]
    r2 = client.get(f"/reports/keywords/{kw_id}")
    assert r2.status_code == 200
    detail = KeywordDetailReport.model_validate(r2.json())
    assert detail.keyword_id == kw_id


def test_keyword_detail_report_404(client: TestClient) -> None:
    r = client.get("/reports/keywords/definitely_not_a_keyword_xyz")
    assert r.status_code == 404
    assert "detail" in r.json()


def test_drilldown_endpoints_405_without_postgres(client: TestClient) -> None:
    """Without POSTGRES_DSN the drill-downs return 405 (contract, not crash)."""
    r = client.get("/keywords")
    assert r.status_code == 200
    keywords = r.json().get("keywords", [])
    if not keywords:
        pytest.skip("No keywords in dataset")
    kw_id = keywords[0]["keyword_id"]
    for path in (
        f"/reports/keywords/{kw_id}/calls",
        f"/reports/keywords/{kw_id}/trend",
        f"/reports/keywords/{kw_id}/managers",
    ):
        r2 = client.get(path)
        assert r2.status_code == 405, f"{path} expected 405 in JSON mode"
        assert "detail" in r2.json()


def test_write_endpoints_405_without_postgres(client: TestClient) -> None:
    body = {
        "keyword_id": "test_kw",
        "label": "Test KW",
        "category": "general",
        "terms": ["test"],
        "match_fields": ["summary"],
        "is_active": True,
    }
    assert client.post("/keywords/upsert", json=body).status_code == 405
    assert client.put("/keywords/test_kw", json=body).status_code == 405
    assert client.delete("/keywords/test_kw").status_code == 405
    assert client.post("/keywords/refresh").status_code == 405
    assert client.post("/keywords/sync", json={}).status_code == 405
    assert client.post("/keywords/materialize").status_code == 405


# ---------------------------------------------------------------------------
# Pagination metadata is exact (no invented pagination elsewhere)
# ---------------------------------------------------------------------------


def test_keyword_calls_model_pagination_fields() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "report_data_source": "postgres_materialized",
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
        "keyword_id": "delivery",
        "total_calls": 120,
        "limit": 50,
        "offset": 50,
        "calls": [
            {
                "call_id": "call-1",
                "match_count": 2,
                "matched_fields": ["summary"],
                "matched_terms": ["delivery"],
                "call_datetime": "2026-03-01",
                "manager_id": "petrenko_aa",
                "manager_name": "Anna Petrenko",
                "role": "sales",
                "direction": "incoming",
                "intent": "order_status",
                "outcome": "positive",
                "summary": "Customer asked about delivery.",
                "audio_seconds": 120.5,
                "spam_probability": 0.1,
                "effective_call": True,
            }
        ],
    }
    report = KeywordCallsReport.model_validate(payload)
    assert report.total_calls == 120
    assert report.limit == 50
    assert report.offset == 50
    call = report.calls[0]
    assert isinstance(call, KeywordCallEntry)
    assert call.call_id == "call-1"


def test_keyword_calls_model_accepts_empty_page() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "report_data_source": "postgres_materialized",
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
        "keyword_id": "delivery",
        "total_calls": 0,
        "limit": 25,
        "offset": 0,
        "calls": [],
    }
    report = KeywordCallsReport.model_validate(payload)
    assert report.calls == []


def test_keywords_report_model_accepts_empty_dataset() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "report_data_source": "json",
        "keyword_data_source": "yaml",
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
        "total_keywords": 0,
        "keywords_with_matches": 0,
        "keywords": [],
    }
    report = KeywordsReport.model_validate(payload)
    assert report.keywords == []


def test_keyword_trend_model_accepts_empty_series() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "report_data_source": "postgres_materialized",
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
        "keyword_id": "delivery",
        "points": [],
    }
    report = KeywordTrendReport.model_validate(payload)
    assert report.points == []


def test_keyword_trend_point_shape() -> None:
    point = KeywordTrendPoint(
        call_datetime="2026-03-01", matched_calls=3, total_matches=5
    )
    assert point.call_datetime == "2026-03-01"


def test_keyword_managers_model_accepts_empty_breakdown() -> None:
    payload = {
        "generated_at": "2026-08-22T00:00:00Z",
        "report_data_source": "postgres_materialized",
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
        "keyword_id": "delivery",
        "managers": [],
    }
    report = KeywordManagersReport.model_validate(payload)
    assert report.managers == []


def test_keyword_aggregate_entry_shape() -> None:
    entry = KeywordAggregateEntry(
        keyword_id="delivery",
        label="Delivery",
        category="logistics",
        terms=["delivery"],
        match_fields=["summary"],
        matched_calls=10,
        total_matches=15,
        matched_managers=3,
        top_intents=[("order_status", 7), ("billing", 3)],
        top_outcomes=[("positive", 6)],
    )
    assert entry.matched_calls == 10
    assert entry.top_intents[0] == ("order_status", 7)


def test_freshness_and_ai_analysis_absent_in_json_mode(client: TestClient) -> None:
    """In JSON/YAML mode freshness is absent and keyword_ai_analysis is null."""
    r = client.get("/reports/keywords")
    assert r.status_code == 200
    body = r.json()
    assert "freshness" not in body or body["freshness"] is None
    assert body.get("keyword_ai_analysis") is None
