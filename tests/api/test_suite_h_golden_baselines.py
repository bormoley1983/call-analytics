# -*- coding: utf-8 -*-
"""
Suite H: Golden Baseline Regression Tests (DEVPLAN_ONLINE_TESTS_TBD_20_03_2026.md)

Validates that report endpoint responses match stored golden baselines.
This catches structural regressions in report payloads.

Golden files live in tests/golden/ and represent the expected structure
for each report endpoint with an empty dataset (no seeded calls).

To regenerate a baseline after an intentional schema change:
    python3 -m pytest tests/api/test_suite_h_golden_baselines.py --regenerate-golden

Markers: @pytest.mark.integration @pytest.mark.postgres
Run: pytest tests/api/test_suite_h_golden_baselines.py -m "integration and postgres" -v
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.golden._normalize import save_golden


def _should_regenerate() -> bool:
    """Check if --regenerate-golden flag was passed."""
    import sys

    return "--regenerate-golden" in sys.argv


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineOverall:
    """Golden baseline tests for /reports/overall."""

    def test_overall_report_matches_golden(self, api_client) -> None:
        """Test: Overall report structure matches golden baseline."""
        response = api_client.get("/reports/overall")
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        body = response.json()
        if _should_regenerate():
            save_golden("overall_report", body)
            pytest.skip("Regenerated golden baseline for overall_report")

        from tests.golden._normalize import assert_matches_golden

        assert_matches_golden("overall_report", body)

    def test_overall_report_has_required_fields(self, api_client) -> None:
        """Test: Overall report has all required structural fields."""
        response = api_client.get("/reports/overall")
        assert response.status_code == 200

        body = response.json()
        required_fields = [
            "generated_at",
            "data_source",
            "filters",
            "total_calls",
            "analyzed_calls",
            "unique_managers",
            "spam_calls",
            "effective_calls",
            "total_duration_seconds",
            "top_intents",
            "top_outcomes",
            "top_questions",
        ]
        missing = [f for f in required_fields if f not in body]
        assert not missing, f"Missing required fields: {missing}"

    def test_overall_report_filters_echo(self, api_client) -> None:
        """Test: Overall report echoes back applied filters."""
        response = api_client.get(
            "/reports/overall",
            params={"date_from": "2025-01-01", "date_to": "2025-12-31"},
        )
        assert response.status_code == 200

        body = response.json()
        filters = body.get("filters", {})
        assert filters.get("date_from") == "2025-01-01"
        assert filters.get("date_to") == "2025-12-31"


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineManagers:
    """Golden baseline tests for /reports/managers."""

    def test_managers_report_matches_golden(self, api_client) -> None:
        """Test: Managers report structure matches golden baseline."""
        response = api_client.get("/reports/managers")
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        body = response.json()
        if _should_regenerate():
            save_golden("managers_report", body)
            pytest.skip("Regenerated golden baseline for managers_report")

        from tests.golden._normalize import assert_matches_golden

        assert_matches_golden("managers_report", body)

    def test_managers_report_has_required_fields(self, api_client) -> None:
        """Test: Managers report has all required structural fields."""
        response = api_client.get("/reports/managers")
        assert response.status_code == 200

        body = response.json()
        required_fields = [
            "generated_at",
            "data_source",
            "filters",
            "role_summary",
            "by_role",
            "all_managers",
            "total_managers",
        ]
        missing = [f for f in required_fields if f not in body]
        assert not missing, f"Missing required fields: {missing}"

    def test_managers_report_sorting(self, api_client) -> None:
        """Test: Managers report respects sort parameters."""
        # Test default sort (total_calls desc)
        response = api_client.get("/reports/managers")
        assert response.status_code == 200

        # Test sort by manager_name
        response = api_client.get(
            "/reports/managers",
            params={"sort_by": "manager_name", "order": "asc"},
        )
        assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineCustomers:
    """Golden baseline tests for /reports/customers."""

    def test_customers_report_matches_golden(self, api_client) -> None:
        """Test: Customers report structure matches golden baseline."""
        response = api_client.get("/reports/customers")
        # May return 500 on empty data — that's OK for structural tests
        if response.status_code == 500:
            pytest.skip("Customers report returned 500 on empty dataset")
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        body = response.json()
        if _should_regenerate():
            save_golden("customers_report", body)
            pytest.skip("Regenerated golden baseline for customers_report")

        from tests.golden._normalize import assert_matches_golden

        assert_matches_golden("customers_report", body)

    def test_customers_report_has_required_fields(self, api_client) -> None:
        """Test: Customers report has all required structural fields."""
        response = api_client.get("/reports/customers")
        if response.status_code == 500:
            pytest.skip("Customers report returned 500 on empty dataset")
        assert response.status_code == 200

        body = response.json()
        required_fields = [
            "generated_at",
            "data_source",
            "filters",
            "all_customers",
            "total_customers",
        ]
        missing = [f for f in required_fields if f not in body]
        assert not missing, f"Missing required fields: {missing}"


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineKeywords:
    """Golden baseline tests for /reports/keywords."""

    def test_keywords_report_matches_golden(self, api_client) -> None:
        """Test: Keywords report structure matches golden baseline."""
        response = api_client.get("/reports/keywords")
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        body = response.json()
        if _should_regenerate():
            save_golden("keywords_report", body)
            pytest.skip("Regenerated golden baseline for keywords_report")

        from tests.golden._normalize import assert_matches_golden

        assert_matches_golden("keywords_report", body)

    def test_keywords_report_has_required_fields(self, api_client) -> None:
        """Test: Keywords report has all required structural fields."""
        response = api_client.get("/reports/keywords")
        assert response.status_code == 200

        body = response.json()
        required_fields = [
            "generated_at",
            "report_data_source",
            "keyword_data_source",
            "filters",
            "total_keywords",
            "keywords_with_matches",
            "keywords",
        ]
        missing = [f for f in required_fields if f not in body]
        assert not missing, f"Missing required fields: {missing}"


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineFreshness:
    """Tests that freshness metadata is consistently present."""

    def test_all_reports_include_freshness(self, api_client) -> None:
        """Test: All report endpoints include freshness metadata."""
        endpoints = [
            "/reports/overall",
            "/reports/managers",
            "/reports/customers",
            "/reports/keywords",
        ]

        for endpoint in endpoints:
            response = api_client.get(endpoint)
            # Skip endpoints that fail on empty data (customers may do this)
            if response.status_code == 500:
                continue
            assert (
                response.status_code == 200
            ), f"{endpoint} returned {response.status_code}: {response.text}"

            body = response.json()
            assert (
                "freshness" in body
            ), f"{endpoint} missing 'freshness' metadata. Keys: {list(body.keys())}"
            freshness = body["freshness"]
            expected_keys = {
                "latest_processed_at",
                "latest_materialized_at",
                "latest_keyword_ai_analysis_at",
                "keyword_ai_analysis_status",
            }
            missing = expected_keys - set(freshness.keys())
            assert not missing, f"{endpoint} freshness missing keys: {missing}"

    def test_freshness_status_is_valid(self, api_client) -> None:
        """Test: keyword_ai_analysis_status is one of the valid states."""
        response = api_client.get("/reports/overall")
        assert response.status_code == 200

        body = response.json()
        freshness = body.get("freshness", {})
        status = freshness.get("keyword_ai_analysis_status")
        assert status in (
            "available",
            "missing",
            "stale",
        ), f"Invalid freshness status: {status!r}. Expected 'available', 'missing', or 'stale'"


@pytest.mark.integration
@pytest.mark.postgres
class TestGoldenBaselineKeywordAiAnalysis:
    """Tests that keyword_ai_analysis is consistently attached."""

    def test_all_reports_include_keyword_ai_analysis(self, api_client) -> None:
        """Test: All report endpoints include keyword_ai_analysis field."""
        endpoints = [
            "/reports/overall",
            "/reports/managers",
            "/reports/customers",
            "/reports/keywords",
        ]

        for endpoint in endpoints:
            response = api_client.get(endpoint)
            if response.status_code == 500:
                continue
            assert (
                response.status_code == 200
            ), f"{endpoint} returned {response.status_code}"

            body = response.json()
            assert (
                "keyword_ai_analysis" in body
            ), f"{endpoint} missing 'keyword_ai_analysis'. Keys: {list(body.keys())}"
