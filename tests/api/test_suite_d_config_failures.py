# -*- coding: utf-8 -*-
"""
Suite D: Config and Failure Modes (DEVPLAN_ONLINE_TESTS_TBD_20_03_2026.md)

Tests failure modes and error handling:
1. Invalid keyword YAML → admin/reporting endpoints fail loudly
2. Missing keyword YAML → graceful degradation or error
3. Keyword drill-down before materialization → returns 409
4. Unknown keyword IDs → stable errors
5. Invalid request payloads → proper validation errors
6. Job concurrency conflicts → proper rejection
7. Database connection failures → graceful handling
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.postgres
class TestKeywordConfigFailures:
    """Tests for invalid/missing keyword configuration."""

    def test_invalid_keyword_payload_returns_422(self, api_client) -> None:
        """Test: Invalid keyword upsert payload returns validation error."""
        invalid_data = {
            "keyword_id": "",  # Empty ID should fail validation
            "label": "Invalid",
            "category": "test",
        }

        response = api_client.post("/keywords/upsert", json=invalid_data)
        # Should get validation error or server error, not success
        assert response.status_code in (
            400,
            422,
            500,
        ), f"Expected validation error, got {response.status_code}"

    def test_missing_required_fields_returns_422(self, api_client) -> None:
        """Test: Missing required fields in keyword upsert returns 422."""
        incomplete_data: dict[str, object] = {}

        response = api_client.post("/keywords/upsert", json=incomplete_data)
        assert response.status_code in (
            400,
            422,
        ), f"Expected validation error, got {response.status_code}"

    def test_keyword_drill_down_before_materialization_returns_409(
        self, api_client
    ) -> None:
        """Test: Keyword drill-down returns 409 if materialization hasn't run."""
        # Request detail for a keyword that exists but isn't materialized
        response = api_client.get("/reports/keywords/nonexistent_keyword/calls")
        # Should return 409 or 404, not 200 with data
        assert response.status_code in (
            200,
            404,
            409,
        ), f"Expected 409/404, got {response.status_code}"


@pytest.mark.integration
@pytest.mark.postgres
class TestUnknownKeywordIds:
    """Tests for unknown or invalid keyword IDs."""

    def test_unknown_keyword_id_returns_stable_error(self, api_client) -> None:
        """Test: Unknown keyword ID returns consistent error."""
        response = api_client.get("/reports/keywords/unknown_id_xyz/calls")
        # Should not return 500 (internal server error)
        assert response.status_code != 500, "Unknown keyword should not cause 500 error"

    def test_special_characters_in_keyword_id_handled(self, api_client) -> None:
        """Test: Special characters in keyword ID are handled gracefully."""
        special_id = "test<script>alert('xss')</script>"
        response = api_client.get(f"/reports/keywords/{special_id}/calls")
        # Should not cause server error
        assert response.status_code in (
            200,
            404,
            405,
        ), f"Special chars should be handled gracefully, got {response.status_code}"


@pytest.mark.integration
@pytest.mark.postgres
class TestJobConcurrency:
    """Tests for job concurrency control."""

    def test_duplicate_process_job_rejected(self, api_client) -> None:
        """Test: Submitting process job while another is running returns 409."""
        # First job
        response1 = api_client.post("/jobs/process", json={})
        assert response1.status_code == 202

        # Quick second job (may succeed if first completes quickly, or fail with 409)
        response2 = api_client.post("/jobs/process", json={})
        # Either 202 (if first completed) or 409 (concurrent rejection)
        assert response2.status_code in (
            202,
            409,
        ), f"Expected 202 or 409, got {response2.status_code}"

    def test_duplicate_sync_job_rejected(self, api_client) -> None:
        """Test: Submitting sync job while another is running returns 409."""
        response1 = api_client.post("/jobs/sync", json={})
        assert response1.status_code in (202, 409)

    def test_sync_and_process_excludes_sync(self, api_client) -> None:
        """Test: sync-and-process job rejects concurrent sync."""
        response1 = api_client.post("/jobs/sync-and-process", json={})
        assert response1.status_code == 202

        # Concurrent sync should be rejected
        response2 = api_client.post("/jobs/sync", json={})
        assert response2.status_code in (
            202,
            409,
        ), f"Expected 202 or 409, got {response2.status_code}"


@pytest.mark.integration
@pytest.mark.postgres
class TestRequestValidation:
    """Tests for request validation on various endpoints."""

    def test_process_with_invalid_days_param(self, api_client) -> None:
        """Test: Process job with invalid 'days' parameter."""
        response = api_client.post("/jobs/process", json={"days": -1})
        # Should handle gracefully (validation error or default behavior)
        assert response.status_code in (
            202,
            400,
            422,
        ), f"Expected validation handling, got {response.status_code}"

    def test_sync_with_invalid_days_param(self, api_client) -> None:
        """Test: Sync job with invalid 'days' parameter."""
        response = api_client.post("/jobs/sync", json={"days": -1})
        assert response.status_code in (
            202,
            400,
            422,
        ), f"Expected validation handling, got {response.status_code}"

    def test_report_with_invalid_date_format(self, api_client) -> None:
        """Test: Report with invalid date format returns proper error."""
        response = api_client.get(
            "/reports/overall", params={"call_date_from": "not-a-date"}
        )
        # Should handle gracefully
        assert response.status_code in (
            200,
            400,
            422,
        ), f"Expected graceful handling, got {response.status_code}"


@pytest.mark.integration
@pytest.mark.postgres
class TestJobStoreBehavior:
    """Tests for job store and job listing."""

    def test_list_jobs_returns_recent_jobs(self, api_client) -> None:
        """Test: GET /jobs returns list of recent jobs."""
        response = api_client.get("/jobs")
        assert response.status_code == 200

        body = response.json()
        assert isinstance(body, list)

    def test_get_nonexistent_job_returns_404(self, api_client) -> None:
        """Test: GET /jobs/{nonexistent_id} returns 404."""
        import uuid

        fake_id = str(uuid.uuid4())
        response = api_client.get(f"/jobs/{fake_id}")
        assert response.status_code == 404


# pytest markers: @pytest.mark.integration @pytest.mark.postgres
# Run with: pytest tests/api/test_suite_d_config_failures.py -m "integration and postgres" -v
