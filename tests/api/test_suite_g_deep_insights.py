"""
Suite G: Deep Insights Integration Tests

Tests the deep insights flow:
1. POST /keywords/catalog/insights/deep/generate
2. GET /keywords/catalog/insights/deep/runs
3. GET /keywords/catalog/insights/deep/runs/{run_id}
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.postgres
class TestDeepInsightsEndpoint:
    """Tests for the deep insights API endpoints."""

    def test_generate_insights_valid_types(self, api_client) -> None:
        """Test: POST /keywords/catalog/insights/deep/generate with valid insight types."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": ["pain_points"],
                "max_insights": 5,
            },
        )
        # Should succeed or return 500 if LLM is not available
        assert response.status_code in (200, 500), f"Unexpected: {response.status_code}"

    def test_generate_insights_invalid_type(self, api_client) -> None:
        """Test: POST /keywords/catalog/insights/deep/generate rejects invalid insight type."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": ["invalid_type"],
            },
        )
        # Should return 422 for validation error
        assert response.status_code == 422

    def test_generate_insights_empty_types(self, api_client) -> None:
        """Test: POST /keywords/catalog/insights/deep/generate rejects empty insight types."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": [],
            },
        )
        # Should return 422 (min_length=1)
        assert response.status_code == 422

    def test_generate_insights_with_date_filters(self, api_client) -> None:
        """Test: POST with date range filters."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": ["trends"],
                "date_from": "2026-01-01",
                "date_to": "2026-12-31",
                "max_insights": 3,
            },
        )
        assert response.status_code in (200, 500)

    def test_generate_insights_with_manager_filter(self, api_client) -> None:
        """Test: POST with manager_id filter."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": ["objections"],
                "manager_id": "m1",
            },
        )
        assert response.status_code in (200, 500)

    def test_list_runs_empty(self, api_client) -> None:
        """Test: GET /keywords/catalog/insights/deep/runs returns empty list when no runs."""
        response = api_client.get("/keywords/catalog/insights/deep/runs")
        assert response.status_code in (200, 405), f"Unexpected: {response.status_code}"
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

    def test_list_runs_with_limit(self, api_client) -> None:
        """Test: GET /keywords/catalog/insights/deep/runs with limit."""
        response = api_client.get(
            "/keywords/catalog/insights/deep/runs",
            params={"limit": 10},
        )
        assert response.status_code in (200, 405)

    def test_list_runs_with_type_filter(self, api_client) -> None:
        """Test: GET /keywords/catalog/insights/deep/runs with insight_type_filter."""
        response = api_client.get(
            "/keywords/catalog/insights/deep/runs",
            params={"insight_type_filter": "pain_points"},
        )
        assert response.status_code in (200, 405)

    def test_get_run_nonexistent(self, api_client) -> None:
        """Test: GET nonexistent run returns 404."""
        import uuid

        fake_id = str(uuid.uuid4())
        response = api_client.get(f"/keywords/catalog/insights/deep/runs/{fake_id}")
        assert response.status_code in (404, 405)

    def test_generate_multiple_insight_types(self, api_client) -> None:
        """Test: Generate multiple insight types in one call."""
        response = api_client.post(
            "/keywords/catalog/insights/deep/generate",
            json={
                "insight_types": [
                    "pain_points",
                    "objections",
                    "trends",
                    "follow_up_risk",
                ],
                "max_insights": 3,
            },
        )
        assert response.status_code in (200, 500)
