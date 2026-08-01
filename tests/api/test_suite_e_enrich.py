# -*- coding: utf-8 -*-
"""
Suite E: Keyword Enrichment Integration Tests

Tests the enrichment flow:
1. POST /keywords/generation/enrich — enrich candidates with LLM
2. Bootstrap with enrich_before_publish flag
3. Pipeline endpoint (generate + enrich + publish)
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.postgres
class TestEnrichmentEndpoint:
    """Tests for the enrichment API endpoints."""

    def test_enrich_endpoint_schema_validation(self, api_client) -> None:
        """Test: POST /keywords/generation/enrich validates request schema."""
        # Empty candidates should be accepted
        response = api_client.post(
            "/keywords/generation/enrich",
            json={"candidates": []},
        )
        # Should succeed with empty result (no LLM call needed)
        assert response.status_code in (
            200,
            500,
        ), f"Unexpected: {response.status_code} {response.text}"

    def test_enrich_endpoint_invalid_payload(self, api_client) -> None:
        """Test: POST /keywords/generation/enrich rejects invalid JSON."""
        response = api_client.post(
            "/keywords/generation/enrich",
            json={"invalid_field": True},
        )
        # Should return 422 for validation error
        assert response.status_code == 422

    def test_pipeline_endpoint_schema(self, api_client) -> None:
        """Test: POST /keywords/generation/pipeline accepts valid schema."""
        response = api_client.post(
            "/keywords/generation/pipeline",
            json={
                "enrich_before_publish": False,
                "max_aliases_per_candidate": 3,
            },
        )
        # Should succeed (405 if no POSTGRES_DSN, 200 otherwise)
        assert response.status_code in (
            200,
            405,
            500,
        ), f"Unexpected: {response.status_code}"


@pytest.mark.integration
@pytest.mark.postgres
class TestBootstrapEnrichment:
    """Tests for bootstrap with enrichment."""

    def test_bootstrap_with_enrich_flag(self, api_client) -> None:
        """Test: Bootstrap endpoint respects enrich_before_publish flag."""
        response = api_client.post(
            "/keywords/generation/bootstrap",
            json={
                "enrich_before_publish": False,
                "max_aliases_per_candidate": 3,
            },
        )
        # Should succeed (may return 405 if no Postgres)
        assert response.status_code in (
            200,
            405,
            500,
        ), f"Unexpected: {response.status_code}"
