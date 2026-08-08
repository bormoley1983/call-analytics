"""Shared job polling utilities for API integration tests."""

from __future__ import annotations

import time
from typing import Any


def poll_job_status(
    api_client, job_id: str, expected_final_status: str, timeout: int = 60
) -> dict[str, Any]:
    """Poll job status until completion or terminal state.

    Returns the last known job body even if the job reached a failed/error state.
    Raises TimeoutError only if the poll loop times out without reaching any terminal state.
    """
    start = time.time()
    while time.time() - start < timeout:
        response = api_client.get(f"/jobs/{job_id}")
        if response.status_code == 200:
            body = response.json()
            status = body.get("status")
            if status == expected_final_status:
                return body
            if status in ("failed", "error"):
                return body  # Return the failed state so callers can inspect it
        time.sleep(2)

    raise TimeoutError(
        f"Job {job_id} did not reach status '{expected_final_status}' within {timeout}s"
    )
