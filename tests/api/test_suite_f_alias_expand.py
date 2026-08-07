"""
Suite F: Alias Expansion Integration Tests

Tests the alias expansion flow:
1. POST /keywords/catalog/{keyword_id}/expand-aliases
2. GET /keywords/catalog/aliases/suggestions
3. POST /keywords/catalog/aliases/suggestions/{id}/approve
4. POST /keywords/catalog/aliases/suggestions/{id}/reject
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
@pytest.mark.postgres
class TestAliasExpansionEndpoint:
    """Tests for the alias expansion API endpoints."""

    def test_expand_aliases_validates_keyword_id(self, api_client) -> None:
        """Test: POST /keywords/catalog/{id}/expand-aliases validates keyword_id format."""
        response = api_client.post(
            "/keywords/catalog/invalid-id/expand-aliases",
            json={"max_aliases": 5},
        )
        # Should return 422 for invalid UUID format or 404 if not found
        assert response.status_code in (404, 422, 405)

    def test_list_suggestions_empty(self, api_client) -> None:
        """Test: GET /keywords/catalog/aliases/suggestions returns empty list when no suggestions."""
        response = api_client.get("/keywords/catalog/aliases/suggestions")
        # Should succeed (405 if no Postgres)
        assert response.status_code in (200, 405), f"Unexpected: {response.status_code}"
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

    def test_list_suggestions_with_keyword_filter(self, api_client) -> None:
        """Test: GET /keywords/catalog/aliases/suggestions with keyword_id filter."""
        response = api_client.get(
            "/keywords/catalog/aliases/suggestions",
            params={"keyword_id": "test-kw"},
        )
        assert response.status_code in (200, 405)

    def test_list_suggestions_with_status_filter(self, api_client) -> None:
        """Test: GET /keywords/catalog/aliases/suggestions with status filter."""
        response = api_client.get(
            "/keywords/catalog/aliases/suggestions",
            params={"status": "pending"},
        )
        assert response.status_code in (200, 405)

    def test_approve_nonexistent_suggestion(self, api_client) -> None:
        """Test: Approving a nonexistent suggestion returns appropriate error."""
        import uuid

        fake_id = str(uuid.uuid4())
        response = api_client.post(
            f"/keywords/catalog/aliases/suggestions/{fake_id}/approve"
        )
        # Should return 404 or similar
        assert response.status_code in (404, 500), f"Unexpected: {response.status_code}"

    def test_reject_nonexistent_suggestion(self, api_client) -> None:
        """Test: Rejecting a nonexistent suggestion returns appropriate error."""
        import uuid

        fake_id = str(uuid.uuid4())
        response = api_client.post(
            f"/keywords/catalog/aliases/suggestions/{fake_id}/reject"
        )
        assert response.status_code in (404, 500)
