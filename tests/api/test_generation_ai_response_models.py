"""BE-04: response-model contract tests for generation and AI workflow endpoints.

Verifies that the ``response_model`` declarations on the BE-04 routes produce
concrete OpenAPI schemas, that stable operation IDs are assigned, that
mutation-risk error responses (405/404/502) are modeled with the shared
``ApiError`` schema, and that actual endpoint output validates against the
models where possible in JSON/YAML mode.

Runs without Postgres (JSON/YAML mode).  Endpoints that require ``POSTGRES_DSN``
return 405 here, which is itself a contract assertion.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
import yaml
from starlette.testclient import TestClient

import api.deps as api_deps
from api.app import app
from api.generation_schemas import (
    AliasSuggestionActionResult,
    DeepInsightRunDetail,
    KeywordAnalysesListResponse,
    KeywordAnalysisDetailResponse,
    KeywordCatalogAnalysisResponse,
    KeywordGenerationCandidatesResponse,
    KeywordGenerationPipelineResponse,
    KeywordPublishResponse,
)
from api.schemas import (
    AIApplyHistoryEntry,
    AIApplyResult,
    DeepInsightResult,
    DeepInsightRunEntry,
    KeywordAliasExpandResult,
    KeywordGenerationEnrichResult,
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
    file with two standard keywords, mirroring the BE-03 pattern.
    """
    keywords_path = tmp_path_factory.mktemp("be04") / "keywords.yaml"
    _write_keywords(keywords_path)

    mp = pytest.MonkeyPatch()
    mp.setattr(api_deps, "get_keywords_config", lambda: keywords_path)
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        mp.undo()


# ---------------------------------------------------------------------------
# OpenAPI schema assertions — generation routes
# ---------------------------------------------------------------------------


def _operation(client: TestClient, path: str, method: str = "get") -> dict:
    spec = client.get("/openapi.json").json()
    return spec["paths"][path][method]


def _schema_ref_name(schema: dict) -> str:
    ref = schema.get("$ref", "")
    return ref.rsplit("/", 1)[-1] if ref else ""


def test_generation_candidates_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/generation/candidates", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordGenerationCandidatesResponse"


def test_generation_publish_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/generation/publish", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordPublishResponse"


def test_generation_bootstrap_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/generation/bootstrap", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordGenerationPipelineResponse"


def test_generation_enrich_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/generation/enrich", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordGenerationEnrichResult"


def test_generation_pipeline_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/generation/pipeline", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordGenerationPipelineResponse"


# ---------------------------------------------------------------------------
# OpenAPI schema assertions — AI catalog routes
# ---------------------------------------------------------------------------


def test_ai_analysis_run_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/analysis", method="post")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordCatalogAnalysisResponse"


def test_ai_analyses_list_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/analyses")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordAnalysesListResponse"


def test_ai_analysis_detail_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/analyses/{analysis_id}")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordAnalysisDetailResponse"


def test_ai_apply_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client, "/keywords/catalog/analyses/{analysis_id}/apply", method="post"
    )
    schema = op["responses"]["201"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "AIApplyResult"


def test_ai_apply_history_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client, "/keywords/catalog/analyses/{analysis_id}/apply/history"
    )
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    # list[AIApplyHistoryEntry] → items.$ref
    items = schema.get("items", {})
    assert _schema_ref_name(items) == "AIApplyHistoryEntry"


def test_alias_expand_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client, "/keywords/catalog/{keyword_id}/expand-aliases", method="post"
    )
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "KeywordAliasExpandResult"


def test_alias_suggestions_list_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/aliases/suggestions")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    items = schema.get("items", {})
    assert _schema_ref_name(items) == "AliasSuggestionEntry"


def test_alias_approve_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client,
        "/keywords/catalog/aliases/suggestions/{suggestion_id}/approve",
        method="post",
    )
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "AliasSuggestionActionResult"


def test_alias_reject_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client,
        "/keywords/catalog/aliases/suggestions/{suggestion_id}/reject",
        method="post",
    )
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "AliasSuggestionActionResult"


def test_insights_generate_openapi_schema(client: TestClient) -> None:
    op = _operation(
        client, "/keywords/catalog/insights/deep/generate", method="post"
    )
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "DeepInsightResult"


def test_insights_runs_list_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/insights/deep/runs")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    items = schema.get("items", {})
    assert _schema_ref_name(items) == "DeepInsightRunEntry"


def test_insights_run_detail_openapi_schema(client: TestClient) -> None:
    op = _operation(client, "/keywords/catalog/insights/deep/runs/{run_id}")
    schema = op["responses"]["200"]["content"]["application/json"]["schema"]
    assert _schema_ref_name(schema) == "DeepInsightRunDetail"


# ---------------------------------------------------------------------------
# Stable operation IDs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "method", "expected_op_id"),
    [
        ("/keywords/generation/candidates", "post", "generation_candidates"),
        ("/keywords/generation/publish", "post", "generation_publish"),
        ("/keywords/generation/bootstrap", "post", "generation_bootstrap"),
        ("/keywords/generation/enrich", "post", "generation_enrich"),
        ("/keywords/generation/pipeline", "post", "generation_pipeline"),
        ("/keywords/catalog/analysis", "post", "ai_analysis_run"),
        ("/keywords/catalog/analyses", "get", "ai_analyses_list"),
        ("/keywords/catalog/analyses/{analysis_id}", "get", "ai_analysis_detail"),
        (
            "/keywords/catalog/analyses/{analysis_id}/apply",
            "post",
            "ai_apply_actions",
        ),
        (
            "/keywords/catalog/analyses/{analysis_id}/apply/history",
            "get",
            "ai_apply_history",
        ),
        ("/keywords/catalog/{keyword_id}/expand-aliases", "post", "alias_expand"),
        ("/keywords/catalog/aliases/suggestions", "get", "alias_suggestions_list"),
        (
            "/keywords/catalog/aliases/suggestions/{suggestion_id}/approve",
            "post",
            "alias_approve",
        ),
        (
            "/keywords/catalog/aliases/suggestions/{suggestion_id}/reject",
            "post",
            "alias_reject",
        ),
        ("/keywords/catalog/insights/deep/generate", "post", "insights_generate"),
        ("/keywords/catalog/insights/deep/runs", "get", "insights_runs_list"),
        (
            "/keywords/catalog/insights/deep/runs/{run_id}",
            "get",
            "insights_run_detail",
        ),
    ],
)
def test_stable_operation_ids(
    client: TestClient, path: str, method: str, expected_op_id: str
) -> None:
    op = _operation(client, path, method)
    assert op["operationId"] == expected_op_id


# ---------------------------------------------------------------------------
# Error responses modeled in OpenAPI (405 / 404 / 502)
# ---------------------------------------------------------------------------


def _error_schema_has_detail(spec: dict, path: str, method: str, status_code: int) -> bool:
    op = spec["paths"][path][method]
    content = op["responses"].get(str(status_code), {}).get("content", {})
    json_content = content.get("application/json", {})
    schema = json_content.get("schema", {})
    if "$ref" in schema:
        return _schema_ref_name(schema) == "ApiError"
    if "properties" in schema:
        return "detail" in schema["properties"]
    example = json_content.get("example", {})
    return isinstance(example, dict) and "detail" in example


def test_error_responses_modeled(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()

    # 405 on generation endpoints (Postgres required)
    for path in (
        "/keywords/generation/candidates",
        "/keywords/generation/publish",
        "/keywords/generation/bootstrap",
        "/keywords/generation/pipeline",
    ):
        assert _error_schema_has_detail(spec, path, "post", 405)

    # 405 on AI analysis history (Postgres required)
    assert _error_schema_has_detail(spec, "/keywords/catalog/analyses", "get", 405)
    assert _error_schema_has_detail(
        spec, "/keywords/catalog/analyses/{analysis_id}", "get", 405
    )

    # 404 on analysis detail
    assert _error_schema_has_detail(
        spec, "/keywords/catalog/analyses/{analysis_id}", "get", 404
    )

    # 502 on AI analysis run (LLM failure)
    assert _error_schema_has_detail(spec, "/keywords/catalog/analysis", "post", 502)

    # 500 on enrich (LLM failure)
    assert _error_schema_has_detail(
        spec, "/keywords/generation/enrich", "post", 500
    )

    # 405 on alias approve/reject (Postgres required)
    for path in (
        "/keywords/catalog/aliases/suggestions/{suggestion_id}/approve",
        "/keywords/catalog/aliases/suggestions/{suggestion_id}/reject",
    ):
        assert _error_schema_has_detail(spec, path, "post", 405)

    # 405 on deep insights runs (Postgres required)
    assert _error_schema_has_detail(
        spec, "/keywords/catalog/insights/deep/runs", "get", 405
    )
    assert _error_schema_has_detail(
        spec, "/keywords/catalog/insights/deep/runs/{run_id}", "get", 405
    )

    # 404 on deep insights run detail
    assert _error_schema_has_detail(
        spec, "/keywords/catalog/insights/deep/runs/{run_id}", "get", 404
    )


# ---------------------------------------------------------------------------
# Dry-run vs applied distinguishable in schema
# ---------------------------------------------------------------------------


def test_ai_apply_result_distinguishes_dry_run(client: TestClient) -> None:
    """AIApplyResult has a required ``dry_run`` bool field."""
    spec = client.get("/openapi.json").json()
    components = spec.get("components", {}).get("schemas", {})
    ai_apply = components.get("AIApplyResult", {})
    props = ai_apply.get("properties", {})
    assert "dry_run" in props
    assert props["dry_run"]["type"] == "boolean"
    # dry_run is required (no default)
    assert "dry_run" in ai_apply.get("required", [])


def test_ai_apply_history_entry_has_dry_run(client: TestClient) -> None:
    """AIApplyHistoryEntry has a ``dry_run`` bool field."""
    spec = client.get("/openapi.json").json()
    components = spec.get("components", {}).get("schemas", {})
    entry = components.get("AIApplyHistoryEntry", {})
    props = entry.get("properties", {})
    assert "dry_run" in props
    assert props["dry_run"]["type"] == "boolean"


# ---------------------------------------------------------------------------
# Partial/skipped results modeled without collapsing to success/failure
# ---------------------------------------------------------------------------


def test_publish_result_models_partial_outcomes(client: TestClient) -> None:
    """KeywordPublishResult has separate created/updated/skipped fields."""
    spec = client.get("/openapi.json").json()
    components = spec.get("components", {}).get("schemas", {})
    publish = components.get("KeywordPublishResult", {})
    props = publish.get("properties", {})
    for field in (
        "created",
        "updated",
        "skipped_existing_terms",
        "skipped_invalid",
        "created_keyword_ids",
        "updated_keyword_ids",
        "skipped_existing_term_values",
    ):
        assert field in props, f"KeywordPublishResult missing field: {field}"


def test_pipeline_response_models_each_stage(client: TestClient) -> None:
    """KeywordGenerationPipelineResponse has independent stage fields."""
    spec = client.get("/openapi.json").json()
    components = spec.get("components", {}).get("schemas", {})
    pipeline = components.get("KeywordGenerationPipelineResponse", {})
    props = pipeline.get("properties", {})
    for field in (
        "filters",
        "generation",
        "enrichment",
        "publish",
        "materialized",
        "keyword_ai_analysis",
        "materialize",
    ):
        assert field in props, f"PipelineResponse missing field: {field}"


def test_ai_apply_result_models_skipped_actions(client: TestClient) -> None:
    """AIApplyResult has actions_skipped with reason for UI explanation."""
    spec = client.get("/openapi.json").json()
    components = spec.get("components", {}).get("schemas", {})
    ai_apply = components.get("AIApplyResult", {})
    props = ai_apply.get("properties", {})
    assert "actions_skipped" in props
    # AISkippedAction has a reason field
    skipped = components.get("AISkippedAction", {})
    skipped_props = skipped.get("properties", {})
    assert "reason" in skipped_props


# ---------------------------------------------------------------------------
# Endpoint output validates against models (real payloads, JSON/YAML mode)
# ---------------------------------------------------------------------------


def test_generation_candidates_405_without_postgres(client: TestClient) -> None:
    """Without POSTGRES_DSN the generation endpoints return 405."""
    body = {"min_support_calls": 1, "min_total_matches": 1}
    r = client.post("/keywords/generation/candidates", json=body)
    assert r.status_code == 405
    assert "detail" in r.json()


def test_generation_publish_405_without_postgres(client: TestClient) -> None:
    body = {"candidates": [{"phrase": "test phrase"}]}
    r = client.post("/keywords/generation/publish", json=body)
    assert r.status_code == 405
    assert "detail" in r.json()


def test_generation_bootstrap_405_without_postgres(client: TestClient) -> None:
    r = client.post("/keywords/generation/bootstrap", json={})
    assert r.status_code == 405
    assert "detail" in r.json()


def test_generation_pipeline_405_without_postgres(client: TestClient) -> None:
    r = client.post("/keywords/generation/pipeline", json={})
    assert r.status_code == 405
    assert "detail" in r.json()


def test_ai_analyses_list_405_without_postgres(client: TestClient) -> None:
    r = client.get("/keywords/catalog/analyses")
    assert r.status_code == 405
    assert "detail" in r.json()


def test_ai_analysis_detail_405_without_postgres(client: TestClient) -> None:
    r = client.get("/keywords/catalog/analyses/11111111-1111-1111-1111-111111111111")
    assert r.status_code == 405
    assert "detail" in r.json()


def test_alias_approve_405_without_postgres(client: TestClient) -> None:
    r = client.post(
        "/keywords/catalog/aliases/suggestions/11111111-1111-1111-1111-111111111111/approve"
    )
    assert r.status_code == 405
    assert "detail" in r.json()


def test_alias_reject_405_without_postgres(client: TestClient) -> None:
    r = client.post(
        "/keywords/catalog/aliases/suggestions/11111111-1111-1111-1111-111111111111/reject"
    )
    assert r.status_code == 405
    assert "detail" in r.json()


def test_insights_runs_list_405_without_postgres(client: TestClient) -> None:
    r = client.get("/keywords/catalog/insights/deep/runs")
    assert r.status_code == 405
    assert "detail" in r.json()


def test_insights_run_detail_405_without_postgres(client: TestClient) -> None:
    r = client.get("/keywords/catalog/insights/deep/runs/some-run-id")
    assert r.status_code == 405
    assert "detail" in r.json()


# ---------------------------------------------------------------------------
# Model validation with synthetic payloads (no live endpoint needed)
# ---------------------------------------------------------------------------


def test_candidates_response_model_validates() -> None:
    payload = {
        "processed_calls": 120,
        "candidate_count": 2,
        "excluded_existing_terms": True,
        "skipped_existing_term_hits": 5,
        "selected_match_fields": ["summary", "key_questions"],
        "candidates": [
            {
                "candidate_id": "cand_delivery_delay",
                "phrase": "delivery delay",
                "support_calls": 10,
                "total_matches": 15,
                "sample_call_ids": ["call-1", "call-2"],
                "suggested_keyword_id": "delivery_delay",
                "suggested_label": "Delivery Delay",
                "suggested_match_fields": ["summary", "key_questions"],
            }
        ],
        "filters": {"date_from": None, "effective_only": True},
    }
    resp = KeywordGenerationCandidatesResponse.model_validate(payload)
    assert resp.candidate_count == 2
    assert len(resp.candidates) == 1
    assert resp.candidates[0].phrase == "delivery delay"


def test_publish_response_model_validates() -> None:
    payload = {
        "publish": {
            "created": 3,
            "updated": 1,
            "skipped_existing_terms": 2,
            "skipped_invalid": 0,
            "created_keyword_ids": ["kw1", "kw2", "kw3"],
            "updated_keyword_ids": ["kw4"],
            "skipped_existing_term_values": ["existing1", "existing2"],
        },
        "materialized": True,
        "materialize": {"matched_calls": 50},
    }
    resp = KeywordPublishResponse.model_validate(payload)
    assert resp.publish.created == 3
    assert resp.materialized is True
    assert resp.materialize is not None


def test_pipeline_response_model_validates() -> None:
    payload = {
        "filters": {"effective_only": True},
        "generation": {
            "processed_calls": 100,
            "candidate_count": 5,
            "excluded_existing_terms": True,
            "skipped_existing_term_hits": 3,
            "selected_match_fields": ["summary"],
            "candidates": [],
        },
        "enrichment": None,
        "publish": {
            "created": 5,
            "updated": 0,
            "skipped_existing_terms": 0,
            "skipped_invalid": 0,
            "created_keyword_ids": ["a", "b", "c", "d", "e"],
            "updated_keyword_ids": [],
            "skipped_existing_term_values": [],
        },
        "materialized": False,
        "keyword_ai_analysis": None,
    }
    resp = KeywordGenerationPipelineResponse.model_validate(payload)
    assert resp.generation.candidate_count == 5
    assert resp.enrichment is None
    assert resp.materialized is False


def test_catalog_analysis_response_model_validates() -> None:
    payload = {
        "keyword_source": "yaml",
        "reporting_source": None,
        "analyzed_keywords": 2,
        "total_candidates_before_limit": 2,
        "truncated": False,
        "keywords": [
            {
                "keyword_id": "delivery",
                "label": "Delivery",
                "category": "logistics",
                "terms": ["delivery"],
                "match_fields": ["summary"],
                "is_active": True,
                "matched_calls": 10,
                "total_matches": 15,
                "matched_managers": 3,
                "top_intents": [("order_status", 5)],
                "top_outcomes": [("success", 4)],
            }
        ],
        "customer_context": [],
        "ai_analysis": {
            "summary": "Two keywords analyzed.",
            "groups": [
                {
                    "group_label": "Logistics",
                    "theme": "Delivery issues",
                    "keywords": ["delivery"],
                    "primary_keyword_id": "delivery",
                    "suggested_category": None,
                    "suggested_shared_terms": [],
                    "suggested_actions": [],
                    "rationale": "Single keyword group.",
                }
            ],
            "ungrouped_keyword_ids": [],
            "global_recommendations": ["Consider adding refund aliases."],
        },
        "analysis_history": None,
    }
    resp = KeywordCatalogAnalysisResponse.model_validate(payload)
    assert resp.analyzed_keywords == 2
    assert len(resp.ai_analysis.groups) == 1
    assert resp.analysis_history is None


def test_analyses_list_response_model_validates() -> None:
    payload = {
        "returned": 1,
        "analyses": [
            {
                "analysis_id": "11111111-1111-1111-1111-111111111111",
                "keyword_source": "postgres",
                "reporting_source": "postgres",
                "ai_model": "llama3",
                "ai_summary": "Test summary",
                "analyzed_keywords": 10,
                "total_candidates_before_limit": 12,
                "truncated": True,
                "created_at": "2026-08-22T10:00:00Z",
            }
        ],
    }
    resp = KeywordAnalysesListResponse.model_validate(payload)
    assert resp.returned == 1
    assert resp.analyses[0].truncated is True


def test_analysis_detail_response_model_validates() -> None:
    payload = {
        "analysis_id": "11111111-1111-1111-1111-111111111111",
        "keyword_source": "postgres",
        "reporting_source": "postgres",
        "ai_model": "llama3",
        "ai_summary": "Test summary",
        "analyzed_keywords": 10,
        "total_candidates_before_limit": 12,
        "truncated": True,
        "request": {"max_keywords": 10},
        "analysis_input": {"keywords": []},
        "ai_analysis": {
            "summary": "Test",
            "groups": [],
            "ungrouped_keyword_ids": [],
            "global_recommendations": [],
        },
        "created_at": "2026-08-22T10:00:00Z",
        "items": {
            "keywords": [
                {"item_key": "delivery", "data": {}, "created_at": "2026-08-22T10:00:00Z"}
            ]
        },
    }
    resp = KeywordAnalysisDetailResponse.model_validate(payload)
    assert resp.analyzed_keywords == 10
    assert "keywords" in resp.items


def test_alias_action_result_model_validates() -> None:
    approve = AliasSuggestionActionResult(
        suggestion_id="abc-123", approved=True, rejected=None, success=True
    )
    assert approve.approved is True
    assert approve.rejected is None

    reject = AliasSuggestionActionResult(
        suggestion_id="abc-123", approved=None, rejected=True, success=True
    )
    assert reject.approved is None
    assert reject.rejected is True


def test_deep_insight_run_detail_model_validates() -> None:
    payload = {
        "run": {
            "run_id": "run-1",
            "ai_model": "llama3",
            "insight_types": ["pain_points"],
            "request_data": {},
            "created_at": "2026-08-22T10:00:00Z",
        },
        "insights": [
            {
                "insight_id": "ins-1",
                "insight_type": "pain_points",
                "title": "Delivery delays",
                "description": "Customers complain about slow delivery.",
                "severity": "high",
                "affected_calls_count": 20,
                "evidence_summary": "Multiple mentions of delay.",
                "metadata": {},
                "created_at": "2026-08-22T10:00:00Z",
            }
        ],
    }
    resp = DeepInsightRunDetail.model_validate(payload)
    assert len(resp.insights) == 1
    assert resp.run["run_id"] == "run-1"


def test_enrich_result_model_validates() -> None:
    payload = {
        "enriched_candidates": [
            {
                "candidate_id": "cand_1",
                "phrase": "delivery delay",
                "suggested_label": "Delivery Delay",
                "suggested_category": "logistics",
                "suggested_aliases": ["late delivery"],
                "merged_with": [],
                "confidence_score": 0.9,
                "reason": "Strong support.",
                "support_calls": 10,
                "total_matches": 15,
                "sample_call_ids": ["call-1"],
            }
        ],
        "original_count": 1,
        "enriched_count": 1,
        "merged_count": 0,
    }
    resp = KeywordGenerationEnrichResult.model_validate(payload)
    assert resp.enriched_count == 1
    assert resp.enriched_candidates[0].suggested_category == "logistics"


def test_deep_insight_result_model_validates() -> None:
    payload = {
        "run_id": "run-1",
        "insight_counts": {"pain_points": 2},
        "insights": [
            {
                "insight_type": "pain_points",
                "title": "Delivery delays",
                "description": "Customers complain.",
                "severity": "high",
                "affected_calls_count": 20,
                "evidence_summary": "Multiple mentions.",
            }
        ],
    }
    resp = DeepInsightResult.model_validate(payload)
    assert resp.insight_counts["pain_points"] == 2


def test_deep_insight_run_entry_model_validates() -> None:
    payload = {
        "run_id": "run-1",
        "ai_model": "llama3",
        "insight_types": ["pain_points", "trends"],
        "total_insights": 5,
        "created_at": "2026-08-22T10:00:00Z",
    }
    resp = DeepInsightRunEntry.model_validate(payload)
    assert resp.total_insights == 5


def test_ai_apply_result_model_validates_dry_run() -> None:
    payload = {
        "apply_id": "apply-1",
        "analysis_id": "analysis-1",
        "applied_at": "2026-08-22T10:00:00Z",
        "dry_run": True,
        "actions_applied": [],
        "actions_skipped": [
            {"action": {"keyword_id": "kw1"}, "reason": "Keyword not found"}
        ],
        "mutations": [
            {
                "action_type": "rename",
                "keyword_id": "kw2",
                "detail": {"suggested_label": "New Label"},
            }
        ],
        "keyword_refreshed": False,
        "follow_up_ran": False,
    }
    resp = AIApplyResult.model_validate(payload)
    assert resp.dry_run is True
    assert len(resp.actions_skipped) == 1
    assert resp.actions_skipped[0].reason == "Keyword not found"


def test_ai_apply_history_entry_model_validates() -> None:
    payload = {
        "apply_id": "apply-1",
        "applied_at": "2026-08-22T10:00:00Z",
        "applied_by": "api",
        "dry_run": False,
        "actions_count": 3,
        "mutations_count": 2,
        "keyword_refreshed": True,
        "follow_up_ran": False,
        "error": None,
    }
    resp = AIApplyHistoryEntry.model_validate(payload)
    assert resp.dry_run is False
    assert resp.actions_count == 3
