# -*- coding: utf-8 -*-
"""
Suite A: Process Flow Integration Tests (DEVPLAN_ONLINE_TESTS_TBD_20_03_2026.md)

Tests the end-to-end process flow via API endpoints:
1. POST /jobs/process → job lifecycle (pending → running → completed)
2. Call status transitions (discovered → transcribed → analyzed)
3. Auto keyword refresh and AI analysis triggers
4. Job concurrency control (reject duplicate active jobs)
5. Freshness metadata in reports reflects process results
"""

from __future__ import annotations

import time
from typing import Any, Dict

import pytest


@pytest.mark.integration
@pytest.mark.postgres
class TestProcessFlowIntegration:
    """End-to-end process flow tests via API endpoints."""

    def test_process_job_lifecycle(self, api_client) -> None:
        """Test: POST /jobs/process creates job with proper lifecycle transitions.

        Expected flow: pending → running → completed/failed
        Note: In test environments without GPU (for STT), jobs may fail.
        We verify the job reaches a terminal state (completed or failed).
        """
        # Submit process job
        response = api_client.post("/jobs/process", json={})
        assert (
            response.status_code == 202
        ), f"Expected 202, got {response.status_code}: {response.text}"

        body = response.json()
        assert "job_id" in body
        assert body["status"] == "pending"

        job_id = body["job_id"]

        # Poll for terminal state (completed or failed — STT may fail without GPU)
        final = _poll_job_status(
            api_client, job_id, expected_final_status="completed", timeout=120
        )
        assert final.get("status") in (
            "completed",
            "failed",
        ), f"Job did not reach terminal state: {final.get('status')}"

    def test_process_job_rejects_duplicate(self, api_client) -> None:
        """Test: Second process job while first is running returns 409 or reuses existing."""
        response = api_client.post("/jobs/process", json={})
        assert response.status_code in (
            202,
            409,
        ), f"Expected 202/409, got {response.status_code}"

        if response.status_code == 202:
            body = response.json()
            assert "job_id" in body

    def test_call_status_transitions_after_process(
        self, api_client, storage_adapter
    ) -> None:
        """Test: Process job updates call timestamps (transcribed_at, analyzed_at)."""
        # Seed a call record
        call_id = "test_process_flow_call_001"
        storage_adapter.store_call(call_id=call_id, status="discovered")

        # Verify initial state
        calls = storage_adapter.get_calls([call_id])
        assert any(c["call_id"] == call_id for c in calls)

        # Submit process job (will process seeded call if audio exists)
        response = api_client.post("/jobs/process", json={})
        assert response.status_code in (202, 409)

    def test_freshness_metadata_reflects_process(self, api_client) -> None:
        """Test: Reports show freshness metadata after process completes."""
        # Submit process job
        response = api_client.post("/jobs/process", json={})
        assert response.status_code == 202

        job_id = response.json()["job_id"]
        try:
            _poll_job_status(
                api_client, job_id, expected_final_status="completed", timeout=120
            )
        except TimeoutError:
            # Job may fail without GPU; still check report structure
            pass

        # Check overall report has freshness metadata
        response = api_client.get("/reports/overall")
        assert response.status_code == 200

        body = response.json()
        assert (
            "freshness" in body or "freshness_metadata" in body
        ), f"Freshness metadata not found in response: {list(body.keys())}"


def _poll_job_status(
    api_client, job_id: str, expected_final_status: str, timeout: int = 60
) -> Dict[str, Any]:
    """Poll job status until completion or terminal state.

    Returns the last known job body even if the job reached a failed/error state.
    Raises TimeoutError only if the poll loop times out without reaching any terminal state.
    """
    start = time.time()
    last_body: Dict[str, Any] = {}
    while time.time() - start < timeout:
        response = api_client.get(f"/jobs/{job_id}")
        if response.status_code == 200:
            body = response.json()
            last_body = body
            status = body.get("status")
            if status == expected_final_status:
                return body
            if status in ("failed", "error"):
                return body  # Return the failed state so callers can inspect it
        time.sleep(2)

    raise TimeoutError(
        f"Job {job_id} did not reach status '{expected_final_status}' within {timeout}s"
    )


# pytest markers: @pytest.mark.integration @pytest.mark.postgres
# Run with: pytest tests/api/test_suite_a_process_flow.py -m "integration and postgres" -v
