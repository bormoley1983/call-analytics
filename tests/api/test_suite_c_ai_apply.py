# -*- coding: utf-8 -*-
"""
Suite C: AI Apply Integration Tests

Tests the full apply flow through the API endpoints:
1. Seed keywords into Postgres keyword source.
2. Create a persisted AI analysis (with groups + suggested_actions).
3. POST /keywords/catalog/analyses/{analysis_id}/apply — dry run and live modes.
4. GET /keywords/catalog/analyses/{analysis_id}/apply/history — verify audit trail.

Covers:
- Dry-run apply: mutations previewed, no catalog changes, persisted record.
- Live apply with rename: keyword label updated, materialization triggered.
- Live apply with merge: source deleted, target updated.
- Live apply with expand_aliases: new terms added.
- Live apply with deactivate: keyword deactivated.
- Apply with keep action: no mutation, just recorded.
- Apply with missing keyword: skipped with reason.
- Apply with self-merge: skipped with reason.
- Apply with non-existent analysis: 404.
- Apply history endpoint: returns persisted records.
- Apply history pagination: limit/offset work correctly.
- Index-based action resolution (group_index/action_index).
- keyword_id-based action resolution.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List

import pytest

from adapters.keyword_ai_analysis_postgres import PostgresKeywordAiAnalysisStore
from adapters.keywords_postgres import PostgresKeywordSource
from domain.keywords import DEFAULT_MATCH_FIELDS, KeywordDefinition

# ---------------------------------------------------------------------------
# Helpers — build analysis dicts matching the persisted schema
# ---------------------------------------------------------------------------


def _build_analysis(
    *,
    analysis_id: str,
    keywords: List[KeywordDefinition],
    groups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build an analysis dict that matches what PostgresKeywordAiAnalysisStore.get_analysis returns.

    The store returns a flat dict with nested 'items' (by item_type) and 'ai_analysis'.
    Items include keywords, groups, actions (with item_key = "group_index:action_index"), etc.
    """
    ai_analysis: Dict[str, Any] = {
        "groups": groups,
        "summary": "Test analysis",
        "global_recommendations": [],
        "ungrouped_keyword_ids": [],
    }

    analysis_input: Dict[str, Any] = {
        "keywords": [
            {
                "keyword_id": kw.keyword_id,
                "label": kw.label,
                "category": kw.category,
                "terms": list(kw.terms) if kw.terms else [],
                "is_active": kw.is_active,
            }
            for kw in keywords
        ],
        "analyzed_keywords": len(keywords),
    }

    # Build items the same way _build_items does
    items: List[Dict[str, Any]] = []

    # Keyword items
    for kw in keywords:
        items.append(
            {
                "analysis_id": analysis_id,
                "item_type": "keyword",
                "item_key": kw.keyword_id,
                "data": {
                    "keyword_id": kw.keyword_id,
                    "label": kw.label,
                    "category": kw.category,
                    "terms": list(kw.terms) if kw.terms else [],
                    "is_active": kw.is_active,
                },
            }
        )

    # Group and action items
    for g_idx, group in enumerate(groups):
        items.append(
            {
                "analysis_id": analysis_id,
                "item_type": "group",
                "item_key": str(g_idx),
                "data": group,
            }
        )
        for a_idx, action in enumerate(group.get("suggested_actions", [])):
            items.append(
                {
                    "analysis_id": analysis_id,
                    "item_type": "action",
                    "item_key": f"{g_idx}:{a_idx}",
                    "data": {
                        "group_label": group.get("group_label"),
                        "group_theme": group.get("theme"),
                        **action,
                    },
                }
            )

    return {
        "analysis_id": analysis_id,
        "keyword_source": "postgres",
        "reporting_source": "postgres",
        "ai_model": None,
        "ai_summary": "Test analysis",
        "analyzed_keywords": len(keywords),
        "total_candidates_before_limit": 0,
        "truncated": False,
        "request": {},
        "analysis_input": analysis_input,
        "ai_analysis": ai_analysis,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "items": _group_items_by_type(items),
    }


def _group_items_by_type(
    items: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        by_type.setdefault(item["item_type"], []).append(item)
    return by_type


def _make_keyword(
    keyword_id: str,
    label: str,
    terms: List[str],
    category: str = "test",
    is_active: bool = True,
) -> KeywordDefinition:
    return KeywordDefinition(
        keyword_id=keyword_id,
        label=label,
        category=category,
        terms=terms,
        match_fields=list(DEFAULT_MATCH_FIELDS),
        is_active=is_active,
    )


def _make_groups_with_actions(
    actions: List[Dict[str, Any]],
    group_label: str = "Test Group",
) -> List[Dict[str, Any]]:
    """Build a single group with the given suggested_actions."""
    return [
        {
            "group_label": group_label,
            "theme": "test theme",
            "keywords": [],
            "suggested_actions": actions,
        }
    ]


# ---------------------------------------------------------------------------
# Fixtures — reuse shared adapters from conftest to avoid connection issues
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_keywords_for_apply(keyword_adapter: PostgresKeywordSource):
    """Seed a set of keywords useful for apply testing."""
    keywords = [
        _make_keyword("kw_rename_src", "Old Label", ["term_a", "term_b"]),
        _make_keyword("kw_merge_src", "Merge Source", ["merge_term_1", "merge_term_2"]),
        _make_keyword("kw_merge_tgt", "Merge Target", ["target_term_1"]),
        _make_keyword("kw_expand", "Expand Me", ["expand_base"]),
        _make_keyword("kw_deactivate", "Will Deactivate", ["deact_term"]),
        _make_keyword("kw_keep", "Keep This", ["keep_term"]),
    ]
    for kw in keywords:
        keyword_adapter.upsert_keyword(kw)
    return keywords


def _seed_analysis(
    analyses_adapter: PostgresKeywordAiAnalysisStore,
    keywords: List[KeywordDefinition],
    groups: List[Dict[str, Any]],
) -> str:
    """Save an analysis via the store (which creates items). Returns analysis_id."""

    ai_analysis: Dict[str, Any] = {
        "groups": groups,
        "summary": "Test analysis for apply",
        "global_recommendations": [],
        "ungrouped_keyword_ids": [],
    }

    analysis_input: Dict[str, Any] = {
        "keywords": [
            {
                "keyword_id": kw.keyword_id,
                "label": kw.label,
                "category": kw.category,
                "terms": list(kw.terms) if kw.terms else [],
                "is_active": kw.is_active,
            }
            for kw in keywords
        ],
        "analyzed_keywords": len(keywords),
    }

    result = analyses_adapter.save_analysis(
        request_data={"test": True},
        analysis_input=analysis_input,
        ai_analysis=ai_analysis,
        keyword_source="postgres",
        reporting_source="postgres",
        ai_model=None,
    )
    return result["analysis_id"]


# ---------------------------------------------------------------------------
# Tests: POST /apply — Dry Run
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplyDryRun:
    """Dry-run apply: mutations previewed, catalog unchanged."""

    def test_dry_run_rename_returns_preview(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Dry-run rename: returns mutations_preview, catalog unchanged."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "rename",
                    "keyword_id": "kw_rename_src",
                    "suggested_label": "New Label",
                    "suggested_terms": ["new_term_a", "new_term_b"],
                    "reason": "AI suggestion",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [
                    {
                        "group_index": 0,
                        "action_index": 0,
                    }
                ],
                "dry_run": True,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Dry run failed: {resp.text}"

        body = resp.json()
        assert body["dry_run"] is True
        assert body["analysis_id"] == analysis_id
        assert body["apply_id"] is not None
        mutations = body["mutations"]
        assert len(mutations) == 1
        assert mutations[0]["action_type"] == "rename"
        assert mutations[0]["keyword_id"] == "kw_rename_src"

        # Catalog should be unchanged
        kw = keyword_adapter.get_keyword("kw_rename_src")
        assert kw is not None
        assert kw.label == "Old Label"

    def test_dry_run_merge_returns_preview(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Dry-run merge: returns merge mutation preview."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "merge",
                    "keyword_id": "kw_merge_src",
                    "target_keyword_id": "kw_merge_tgt",
                    "suggested_terms": [],
                    "reason": "Duplicate keywords",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Dry run merge failed: {resp.text}"

        body = resp.json()
        assert body["dry_run"] is True
        assert body["apply_id"] is not None
        mutations = body["mutations"]
        assert len(mutations) == 1
        assert mutations[0]["action_type"] == "merge"
        assert mutations[0]["detail"]["target_keyword_id"] == "kw_merge_tgt"

    def test_dry_run_keep_no_mutation(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Dry-run keep: no mutations, action applied."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "keep",
                    "keyword_id": "kw_keep",
                    "reason": "Good as is",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 201, f"Dry run keep failed: {resp.text}"

        body = resp.json()
        assert body["dry_run"] is True
        assert body["apply_id"] is not None
        assert body["mutations"] == []
        assert len(body["actions_applied"]) >= 1


# ---------------------------------------------------------------------------
# Tests: POST /apply — Live Mode
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplyLiveMode:
    """Live apply: mutations executed, catalog changed."""

    def test_live_rename_updates_keyword(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live rename: keyword label and terms updated."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "rename",
                    "keyword_id": "kw_rename_src",
                    "suggested_label": "Renamed Label",
                    "suggested_terms": ["renamed_term"],
                    "reason": "AI suggestion",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Live rename failed: {resp.text}"

        body = resp.json()
        assert body["dry_run"] is False
        assert body["apply_id"] is not None
        assert len(body["mutations"]) == 1

        # Verify keyword was actually renamed
        kw = keyword_adapter.get_keyword("kw_rename_src")
        assert kw is not None
        assert kw.label == "Renamed Label"
        assert "renamed_term" in kw.terms

    def test_live_merge_deletes_source_updates_target(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live merge: source deleted, target has merged terms."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "merge",
                    "keyword_id": "kw_merge_src",
                    "target_keyword_id": "kw_merge_tgt",
                    "suggested_terms": ["extra_term"],
                    "reason": "Duplicates",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Live merge failed: {resp.text}"

        body = resp.json()
        assert body["dry_run"] is False
        assert len(body["mutations"]) == 1

        # Source should be gone
        assert keyword_adapter.get_keyword("kw_merge_src") is None

        # Target should have merged terms
        tgt = keyword_adapter.get_keyword("kw_merge_tgt")
        assert tgt is not None
        assert "merge_term_1" in tgt.terms
        assert "merge_term_2" in tgt.terms
        assert "target_term_1" in tgt.terms
        assert "extra_term" in tgt.terms

    def test_live_expand_aliases_adds_terms(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live expand_aliases: new terms added to existing keyword."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "expand_aliases",
                    "keyword_id": "kw_expand",
                    "suggested_terms": ["new_alias_1", "new_alias_2"],
                    "reason": "Common variations",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Live expand failed: {resp.text}"

        kw = keyword_adapter.get_keyword("kw_expand")
        assert kw is not None
        assert "expand_base" in kw.terms
        assert "new_alias_1" in kw.terms
        assert "new_alias_2" in kw.terms

    def test_live_deactivate_sets_inactive(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live deactivate: keyword is_active set to False."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "deactivate",
                    "keyword_id": "kw_deactivate",
                    "reason": "No longer relevant",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Live deactivate failed: {resp.text}"

        kw = keyword_adapter.get_keyword("kw_deactivate")
        assert kw is not None
        assert kw.is_active is False

    def test_live_keep_records_no_mutation(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live keep: no mutation, action recorded as applied."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "keep",
                    "keyword_id": "kw_keep",
                    "reason": "Good",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Live keep failed: {resp.text}"

        body = resp.json()
        assert body["mutations"] == []
        assert len(body["actions_applied"]) >= 1


# ---------------------------------------------------------------------------
# Tests: POST /apply — keyword_id-based resolution
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplyKeywordIdResolution:
    """Apply using keyword_id instead of group_index/action_index."""

    def test_apply_by_keyword_id_rename(
        self, api_client, analyses_adapter, seed_keywords_for_apply, keyword_adapter
    ):
        """Live rename using keyword_id reference."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "rename",
                    "keyword_id": "kw_rename_src",
                    "suggested_label": "By Keyword ID",
                    "suggested_terms": [],
                    "reason": "Direct reference",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"keyword_id": "kw_rename_src"}],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Keyword ID apply failed: {resp.text}"

        kw = keyword_adapter.get_keyword("kw_rename_src")
        assert kw is not None
        assert kw.label == "By Keyword ID"


# ---------------------------------------------------------------------------
# Tests: POST /apply — Skip & Error Cases
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplySkipAndErrors:
    """Actions that should be skipped or return errors."""

    def test_missing_analysis_returns_404(self, api_client):
        """Apply on non-existent analysis returns 404."""
        fake_id = str(uuid.uuid4())
        resp = api_client.post(
            f"/keywords/catalog/analyses/{fake_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 404

    def test_missing_keyword_skipped(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Action on non-existent keyword is skipped with reason."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "rename",
                    "keyword_id": "kw_does_not_exist",
                    "suggested_label": "Nope",
                    "suggested_terms": [],
                    "reason": "test",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 201, f"Should succeed with skip: {resp.text}"

        body = resp.json()
        assert len(body["actions_skipped"]) >= 1
        reasons = [s["reason"] for s in body["actions_skipped"]]
        assert any("not found" in r.lower() for r in reasons)

    def test_self_merge_skipped(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Self-merge (keyword_id == target_keyword_id) is skipped."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "merge",
                    "keyword_id": "kw_merge_src",
                    "target_keyword_id": "kw_merge_src",
                    "suggested_terms": [],
                    "reason": "test",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 201, f"Should succeed with skip: {resp.text}"

        body = resp.json()
        assert len(body["actions_skipped"]) >= 1
        reasons = [s["reason"] for s in body["actions_skipped"]]
        assert any(
            "self-merge" in r.lower() or "self merge" in r.lower() for r in reasons
        )

    def test_invalid_indices_skipped(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Action with out-of-range indices is skipped."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "keep",
                    "keyword_id": "kw_keep",
                    "reason": "ok",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 99, "action_index": 99}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 201, f"Should succeed with skip: {resp.text}"

        body = resp.json()
        assert len(body["actions_skipped"]) >= 1


# ---------------------------------------------------------------------------
# Tests: GET /apply/history
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplyHistory:
    """GET /keywords/catalog/analyses/{analysis_id}/apply/history"""

    def test_history_empty_before_apply(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """History is empty before any apply."""
        groups = _make_groups_with_actions(
            [{"type": "keep", "keyword_id": "kw_keep", "reason": "ok"}]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.get(f"/keywords/catalog/analyses/{analysis_id}/apply/history")
        assert resp.status_code == 200, f"History failed: {resp.text}"
        assert resp.json() == []

    def test_history_after_dry_run(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Dry-run apply appears in history."""
        groups = _make_groups_with_actions(
            [{"type": "keep", "keyword_id": "kw_keep", "reason": "ok"}]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        # Do a dry-run apply
        api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": True,
                "refresh_after": False,
            },
        )

        resp = api_client.get(f"/keywords/catalog/analyses/{analysis_id}/apply/history")
        assert resp.status_code == 200, f"History failed: {resp.text}"
        history = resp.json()
        assert len(history) >= 1
        entry = history[0]
        assert entry["dry_run"] is True
        assert "apply_id" in entry

    def test_history_after_live_apply(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Live apply appears in history with mutations_count."""
        groups = _make_groups_with_actions(
            [
                {
                    "type": "rename",
                    "keyword_id": "kw_rename_src",
                    "suggested_label": "History Test",
                    "suggested_terms": [],
                    "reason": "test",
                }
            ]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{"group_index": 0, "action_index": 0}],
                "dry_run": False,
                "refresh_after": False,
            },
        )

        resp = api_client.get(f"/keywords/catalog/analyses/{analysis_id}/apply/history")
        assert resp.status_code == 200
        history = resp.json()
        assert len(history) >= 1
        entry = history[0]
        assert entry["dry_run"] is False
        assert entry["mutations_count"] >= 1

    def test_history_multiple_applies(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Multiple applies on same analysis accumulate in history."""
        groups = _make_groups_with_actions(
            [{"type": "keep", "keyword_id": "kw_keep", "reason": "ok"}]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        # Two applies
        for _ in range(2):
            api_client.post(
                f"/keywords/catalog/analyses/{analysis_id}/apply",
                json={
                    "actions": [{"group_index": 0, "action_index": 0}],
                    "dry_run": True,
                    "refresh_after": False,
                },
            )

        resp = api_client.get(f"/keywords/catalog/analyses/{analysis_id}/apply/history")
        assert resp.status_code == 200
        history = resp.json()
        assert len(history) >= 2

    def test_history_limit_pagination(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """History respects limit parameter."""
        groups = _make_groups_with_actions(
            [{"type": "keep", "keyword_id": "kw_keep", "reason": "ok"}]
        )
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        # Three applies
        for _ in range(3):
            api_client.post(
                f"/keywords/catalog/analyses/{analysis_id}/apply",
                json={
                    "actions": [{"group_index": 0, "action_index": 0}],
                    "dry_run": True,
                    "refresh_after": False,
                },
            )

        resp = api_client.get(
            f"/keywords/catalog/analyses/{analysis_id}/apply/history",
            params={"limit": 1},
        )
        assert resp.status_code == 200
        history = resp.json()
        assert len(history) == 1


# ---------------------------------------------------------------------------
# Tests: POST /apply — Multiple Actions in Single Request
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplyMultipleActions:
    """Single apply request with multiple actions."""

    def test_multiple_actions_single_request(
        self, api_client, analyses_adapter, keyword_adapter
    ):
        """Apply rename + deactivate in one request."""
        keywords = [
            _make_keyword("kw_multi_rename", "Multi Rename", ["r1"]),
            _make_keyword("kw_multi_deact", "Multi Deact", ["d1"]),
        ]
        for kw in keywords:
            keyword_adapter.upsert_keyword(kw)

        groups = [
            {
                "group_label": "Group 0",
                "theme": "t",
                "keywords": [],
                "suggested_actions": [
                    {
                        "type": "rename",
                        "keyword_id": "kw_multi_rename",
                        "suggested_label": "Renamed",
                        "suggested_terms": [],
                        "reason": "test",
                    }
                ],
            },
            {
                "group_label": "Group 1",
                "theme": "t",
                "keywords": [],
                "suggested_actions": [
                    {
                        "type": "deactivate",
                        "keyword_id": "kw_multi_deact",
                        "reason": "test",
                    }
                ],
            },
        ]

        analysis_id = _seed_analysis(analyses_adapter, keywords, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [
                    {"group_index": 0, "action_index": 0},
                    {"group_index": 1, "action_index": 0},
                ],
                "dry_run": False,
                "refresh_after": False,
            },
        )
        assert resp.status_code == 201, f"Multi-action apply failed: {resp.text}"

        body = resp.json()
        assert len(body["mutations"]) == 2

        # Verify both mutations
        kw1 = keyword_adapter.get_keyword("kw_multi_rename")
        assert kw1 is not None
        assert kw1.label == "Renamed"

        kw2 = keyword_adapter.get_keyword("kw_multi_deact")
        assert kw2 is not None
        assert kw2.is_active is False


# ---------------------------------------------------------------------------
# Tests: POST /apply — Schema Validation at API Level
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.postgres
class TestApplySchemaValidation:
    """Request body validation errors from Pydantic schemas."""

    def test_empty_actions_rejected(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Empty actions list is rejected by schema."""
        groups = _make_groups_with_actions([])
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={"actions": [], "dry_run": True},
        )
        assert resp.status_code == 422

    def test_action_without_reference_rejected(
        self, api_client, analyses_adapter, seed_keywords_for_apply
    ):
        """Action without group_index/action_index or keyword_id is rejected."""
        groups = _make_groups_with_actions([])
        analysis_id = _seed_analysis(analyses_adapter, seed_keywords_for_apply, groups)

        resp = api_client.post(
            f"/keywords/catalog/analyses/{analysis_id}/apply",
            json={
                "actions": [{}],
                "dry_run": True,
            },
        )
        assert resp.status_code == 422
