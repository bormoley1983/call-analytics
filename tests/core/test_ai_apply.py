# -*- coding: utf-8 -*-
"""Unit tests for src/core/ai_apply.py — AI keyword catalog apply logic.

Covers:
- Action resolution from analysis items (index-based and keyword_id-based)
- Validation and mutation building (merge, rename, expand_aliases, deactivate, keep)
- Mutation execution on FakeKeywordSource
- Full orchestration via apply_approved_actions (dry_run and live modes)
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, cast

from adapters.ai_apply_postgres import PostgresAiApplyStore
from api.schemas import AIApplyAction, AIMutation, AISkippedAction
from core.ai_apply import (
    _do_deactivate,
    _do_expand_aliases,
    _do_merge,
    _do_rename,
    _execute_mutations_on_keyword_source,
    _find_action_by_keyword_id,
    _find_action_from_groups,
    _resolve_actions_from_analysis,
    _validate_and_build_mutations,
    apply_approved_actions,
)
from domain.keywords import KeywordDefinition

# ---------------------------------------------------------------------------
# Fake keyword source — in-memory implementation
# ---------------------------------------------------------------------------


class FakeKeywordSource:
    """In-memory keyword source for testing mutation execution."""

    def __init__(self, items: List[KeywordDefinition]):
        self._store: Dict[str, KeywordDefinition] = {
            kw.keyword_id: copy.deepcopy(kw) for kw in items
        }

    def list_keywords(self) -> List[KeywordDefinition]:
        return list(self._store.values())

    def get_keyword(self, keyword_id: str) -> Optional[KeywordDefinition]:
        kw = self._store.get(keyword_id)
        return copy.deepcopy(kw) if kw else None

    def upsert_keyword(self, keyword: KeywordDefinition) -> KeywordDefinition:
        self._store[keyword.keyword_id] = copy.deepcopy(keyword)
        return keyword

    def delete_keyword(self, keyword_id: str) -> bool:
        if keyword_id in self._store:
            del self._store[keyword_id]
            return True
        return False

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Fake apply store — in-memory implementation
# ---------------------------------------------------------------------------


class FakeApplyStore:
    """In-memory apply store for testing persistence."""

    def __init__(self):
        self.apply_history: List[Dict[str, Any]] = []

    def apply_actions(
        self,
        analysis_id: str,
        applied_by: Optional[str],
        dry_run: bool,
        actions_applied: List[Dict],
        actions_skipped: List[Dict],
        mutations: List[Dict],
        keyword_refreshed: bool,
        follow_up_ran: bool,
        error: Optional[str] = None,
    ) -> str:
        import uuid

        apply_id = str(uuid.uuid4())
        record = {
            "apply_id": apply_id,
            "analysis_id": analysis_id,
            "applied_by": applied_by,
            "dry_run": dry_run,
            "actions_applied": actions_applied,
            "actions_skipped": actions_skipped,
            "mutations": mutations,
            "keyword_refreshed": keyword_refreshed,
            "follow_up_ran": follow_up_ran,
            "error": error,
        }
        self.apply_history.append(record)
        return apply_id

    def get_apply_history(
        self, analysis_id: str, limit: int = 50, offset: int = 0
    ) -> List[Dict]:
        return [r for r in self.apply_history if r["analysis_id"] == analysis_id][
            offset : offset + limit
        ]

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kw(
    keyword_id: str,
    label: str = "",
    is_active: bool = True,
    terms: Optional[List[str]] = None,
    category: str = "general",
) -> KeywordDefinition:
    return KeywordDefinition(
        keyword_id=keyword_id,
        label=label or keyword_id.title().replace("_", " "),
        category=category,
        terms=terms or [keyword_id],
        match_fields=["summary"],
        is_active=is_active,
    )


def _make_analysis(groups: List[Dict]) -> Dict[str, Any]:
    """Build a fake analysis dict with items["action"] populated from groups."""

    actions_items = []
    for gi, group in enumerate(groups):
        for ai, action in enumerate(group.get("suggested_actions", [])):
            actions_items.append(
                {
                    "item_key": f"{gi}:{ai}",
                    "data": dict(action),
                    "group": dict(group),
                }
            )

    return {
        "analysis_id": "test-analysis-001",
        "items": {"action": actions_items},
        "ai_analysis": {"groups": groups},
    }


def _make_groups_with_actions(
    group_label: str,
    actions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "group_label": group_label,
            "suggested_actions": actions,
        }
    ]


# ---------------------------------------------------------------------------
# Tests: _resolve_actions_from_analysis
# ---------------------------------------------------------------------------


def test_resolve_actions_index_based():
    groups = _make_groups_with_actions(
        "Logistics",
        [
            {
                "type": "merge",
                "keyword_id": "shipment",
                "target_keyword_id": "delivery",
                "reason": "overlap",
            },
            {
                "type": "rename",
                "keyword_id": "delivery",
                "suggested_label": "Delivery & Shipping",
                "reason": "broader",
            },
        ],
    )
    analysis = _make_analysis(groups)

    resolved = _resolve_actions_from_analysis(
        analysis,
        [
            AIApplyAction(group_index=0, action_index=0),
            AIApplyAction(group_index=0, action_index=1),
        ],
    )

    assert len(resolved) == 2
    assert resolved[0]["type"] == "merge"
    assert resolved[0]["keyword_id"] == "shipment"
    assert resolved[1]["type"] == "rename"
    assert resolved[1]["keyword_id"] == "delivery"


def test_resolve_actions_keyword_id_based():
    groups = _make_groups_with_actions(
        "Logistics",
        [
            {
                "type": "merge",
                "keyword_id": "shipment",
                "target_keyword_id": "delivery",
                "reason": "overlap",
            },
            {"type": "keep", "keyword_id": "delivery", "reason": "primary"},
        ],
    )
    analysis = _make_analysis(groups)

    resolved = _resolve_actions_from_analysis(
        analysis,
        [AIApplyAction(keyword_id="shipment")],
    )

    assert len(resolved) == 1
    assert resolved[0]["type"] == "merge"


def test_resolve_actions_keyword_id_skips_keep():
    groups = _make_groups_with_actions(
        "Logistics",
        [
            {"type": "keep", "keyword_id": "delivery", "reason": "ok"},
            {
                "type": "rename",
                "keyword_id": "delivery",
                "suggested_label": "New Label",
                "reason": "clarify",
            },
        ],
    )
    analysis = _make_analysis(groups)

    resolved = _resolve_actions_from_analysis(
        analysis,
        [AIApplyAction(keyword_id="delivery")],
    )

    assert len(resolved) == 1
    assert resolved[0]["type"] == "rename"


def test_resolve_actions_missing_index_returns_error():
    groups = _make_groups_with_actions("G", [{"type": "keep", "keyword_id": "a"}])
    analysis = _make_analysis(groups)

    resolved = _resolve_actions_from_analysis(
        analysis,
        [AIApplyAction(group_index=99, action_index=99)],
    )

    assert len(resolved) == 1
    assert "_resolve_error" in resolved[0]


def test_resolve_actions_missing_keyword_id_returns_error():
    groups = _make_groups_with_actions("G", [{"type": "keep", "keyword_id": "a"}])
    analysis = _make_analysis(groups)

    resolved = _resolve_actions_from_analysis(
        analysis,
        [AIApplyAction(keyword_id="nonexistent")],
    )

    assert len(resolved) == 1
    assert "_resolve_error" in resolved[0]


def test_resolve_actions_invalid_reference():
    groups: list[dict[str, Any]] = []
    analysis = _make_analysis(groups)

    # No indices and no keyword_id — should be caught by schema validator, but defensive check:
    fake_action = AIApplyAction(group_index=0, action_index=0)
    resolved = _resolve_actions_from_analysis(analysis, [fake_action])

    assert len(resolved) == 1
    assert "_resolve_error" in resolved[0]


# ---------------------------------------------------------------------------
# Tests: _find_action_from_groups
# ---------------------------------------------------------------------------


def test_find_action_from_groups_valid():
    groups = _make_groups_with_actions("G", [{"type": "rename", "keyword_id": "a"}])
    result = _find_action_from_groups(groups, 0, 0)
    assert result is not None
    assert result["type"] == "rename"


def test_find_action_from_groups_out_of_range():
    groups = _make_groups_with_actions("G", [{"type": "keep"}])
    assert _find_action_from_groups(groups, 1, 0) is None
    assert _find_action_from_groups(groups, 0, 5) is None


# ---------------------------------------------------------------------------
# Tests: _find_action_by_keyword_id
# ---------------------------------------------------------------------------


def test_find_action_by_keyword_id_finds_first_non_keep():
    groups = _make_groups_with_actions(
        "G",
        [
            {"type": "keep", "keyword_id": "a"},
            {"type": "deactivate", "keyword_id": "a", "reason": "unused"},
        ],
    )
    result = _find_action_by_keyword_id(groups, "a")
    assert result is not None
    assert result["type"] == "deactivate"


def test_find_action_by_keyword_id_not_found():
    groups = _make_groups_with_actions("G", [{"type": "keep", "keyword_id": "a"}])
    result = _find_action_by_keyword_id(groups, "b")
    assert result is None


# ---------------------------------------------------------------------------
# Tests: _validate_and_build_mutations
# ---------------------------------------------------------------------------


def test_validate_keep_action():
    resolved = [{"type": "keep", "keyword_id": "a", "reason": "ok"}]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"a"}, {})

    assert len(applied) == 1
    assert len(skipped) == 0
    assert len(mutations) == 0


def test_validate_merge_valid():
    resolved = [
        {
            "type": "merge",
            "keyword_id": "source_kw",
            "target_keyword_id": "target_kw",
            "suggested_terms": ["extra"],
            "reason": "overlap",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(
        resolved, {"source_kw", "target_kw"}, {}
    )

    assert len(applied) == 1
    assert len(skipped) == 0
    assert len(mutations) == 1
    assert mutations[0].action_type == "merge"
    assert mutations[0].keyword_id == "source_kw"
    assert mutations[0].detail["target_keyword_id"] == "target_kw"


def test_validate_merge_missing_source():
    resolved = [
        {
            "type": "merge",
            "keyword_id": "missing_kw",
            "target_keyword_id": "target_kw",
            "reason": "overlap",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(
        resolved, {"target_kw"}, {}
    )

    assert len(applied) == 0
    assert len(skipped) == 1
    assert "not found" in skipped[0].reason


def test_validate_merge_missing_target():
    resolved = [
        {
            "type": "merge",
            "keyword_id": "source_kw",
            "target_keyword_id": "missing_target",
            "reason": "overlap",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(
        resolved, {"source_kw"}, {}
    )

    assert len(applied) == 0
    assert len(skipped) == 1
    assert "not found" in skipped[0].reason


def test_validate_merge_self_merge_skipped():
    resolved = [
        {
            "type": "merge",
            "keyword_id": "kw_a",
            "target_keyword_id": "kw_a",
            "reason": "self",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"kw_a"}, {})

    assert len(applied) == 0
    assert len(skipped) == 1
    assert "Self-merge" in skipped[0].reason


def test_validate_rename():
    resolved = [
        {
            "type": "rename",
            "keyword_id": "kw_a",
            "suggested_label": "New Label",
            "suggested_terms": ["new_term"],
            "reason": "clarity",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"kw_a"}, {})

    assert len(mutations) == 1
    assert mutations[0].action_type == "rename"


def test_validate_expand_aliases():
    resolved = [
        {
            "type": "expand_aliases",
            "keyword_id": "kw_a",
            "suggested_terms": ["alias1", "alias2"],
            "reason": "more coverage",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"kw_a"}, {})

    assert len(mutations) == 1
    assert mutations[0].action_type == "expand_aliases"


def test_validate_deactivate():
    resolved = [
        {
            "type": "deactivate",
            "keyword_id": "kw_a",
            "reason": "unused",
        }
    ]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"kw_a"}, {})

    assert len(mutations) == 1
    assert mutations[0].action_type == "deactivate"


def test_validate_unknown_action_type_skipped():
    resolved = [{"type": "unknown_type", "keyword_id": "a"}]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"a"}, {})

    assert len(applied) == 0
    assert len(skipped) == 1
    assert "Unknown action type" in skipped[0].reason


def test_validate_resolve_error_skipped():
    resolved = [{"_resolve_error": "Could not resolve", "keyword_id": "x"}]
    applied, skipped, mutations = _validate_and_build_mutations(resolved, {"x"}, {})

    assert len(applied) == 0
    assert len(skipped) == 1
    assert "Could not resolve" in skipped[0].reason


# ---------------------------------------------------------------------------
# Tests: Mutation execution helpers
# ---------------------------------------------------------------------------


def test_do_merge_combines_terms_and_deletes_source():
    source = FakeKeywordSource(
        [
            _kw("source_kw", terms=["s1", "s2"]),
            _kw("target_kw", terms=["t1", "t2"]),
        ]
    )
    detail = {"target_keyword_id": "target_kw", "suggested_terms": ["shared"]}
    _do_merge(source, "source_kw", detail)

    assert source.get_keyword("source_kw") is None
    target = source.get_keyword("target_kw")
    assert target is not None
    assert set(target.terms) == {"t1", "t2", "s1", "s2", "shared"}


def test_do_merge_skips_when_source_missing():
    source = FakeKeywordSource([_kw("target_kw")])
    detail = {"target_keyword_id": "target_kw", "suggested_terms": []}
    _do_merge(source, "missing_kw", detail)

    assert source.get_keyword("target_kw") is not None
    assert source.get_keyword("missing_kw") is None


def test_do_rename_updates_label_and_terms():
    source = FakeKeywordSource([_kw("my_kw", label="Old Label", terms=["old"])])
    detail = {"suggested_label": "New Label", "suggested_terms": ["new1", "new2"]}
    _do_rename(source, "my_kw", detail)

    kw = source.get_keyword("my_kw")
    assert kw is not None
    assert kw.label == "New Label"
    assert kw.terms == ["new1", "new2"]


def test_do_rename_skips_when_missing():
    source = FakeKeywordSource([])
    detail = {"suggested_label": "X"}
    _do_rename(source, "missing_kw", detail)
    assert source.list_keywords() == []


def test_do_expand_aliases_adds_new_terms():
    source = FakeKeywordSource([_kw("my_kw", terms=["existing"])])
    detail = {"suggested_terms": ["new_alias", "existing"]}
    _do_expand_aliases(source, "my_kw", detail)

    kw = source.get_keyword("my_kw")
    assert kw is not None
    assert set(kw.terms) == {"existing", "new_alias"}


def test_do_deactivate_sets_is_active_false():
    source = FakeKeywordSource([_kw("my_kw", is_active=True)])
    _do_deactivate(source, "my_kw")

    kw = source.get_keyword("my_kw")
    assert kw is not None
    assert kw.is_active is False


def test_do_deactivate_skips_when_missing():
    source = FakeKeywordSource([])
    _do_deactivate(source, "missing_kw")
    assert source.list_keywords() == []


# ---------------------------------------------------------------------------
# Tests: _execute_mutations_on_keyword_source
# ---------------------------------------------------------------------------


def test_execute_mutations_dry_run_no_op():
    source = FakeKeywordSource([_kw("my_kw", is_active=True)])
    mutations = [AIMutation(action_type="deactivate", keyword_id="my_kw", detail={})]
    _execute_mutations_on_keyword_source(mutations, source, dry_run=True)

    kw = source.get_keyword("my_kw")
    assert kw is not None
    assert kw.is_active is True


def test_execute_mutations_live_applies():
    source = FakeKeywordSource([_kw("my_kw", is_active=True)])
    mutations = [AIMutation(action_type="deactivate", keyword_id="my_kw", detail={})]
    _execute_mutations_on_keyword_source(mutations, source, dry_run=False)

    kw = source.get_keyword("my_kw")
    assert kw is not None
    assert kw.is_active is False


# ---------------------------------------------------------------------------
# Tests: apply_approved_actions (full orchestration)
# ---------------------------------------------------------------------------


def test_apply_dry_run_no_mutations_executed():
    groups = _make_groups_with_actions(
        "G",
        [
            {
                "type": "deactivate",
                "keyword_id": "active_kw",
                "reason": "unused",
            }
        ],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource([_kw("active_kw", is_active=True)])
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-001",
        analysis=analysis,
        request_actions=[AIApplyAction(group_index=0, action_index=0)],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=True,
        refresh_after=False,
        applied_by="test_user",
    )

    # Dry run: keyword should NOT be deactivated
    kw = keyword_source.get_keyword("active_kw")
    assert kw is not None
    assert kw.is_active is True

    # Result should indicate dry_run
    assert result["dry_run"] is True
    assert "actions_applied" in result
    assert "mutations" in result


def test_apply_live_mode_executes_mutations():
    groups = _make_groups_with_actions(
        "G",
        [
            {
                "type": "deactivate",
                "keyword_id": "active_kw",
                "reason": "unused",
            }
        ],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource([_kw("active_kw", is_active=True)])
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-001",
        analysis=analysis,
        request_actions=[AIApplyAction(group_index=0, action_index=0)],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=False,
        refresh_after=False,
        applied_by="test_user",
    )

    # Live mode: keyword should be deactivated
    kw = keyword_source.get_keyword("active_kw")
    assert kw is not None
    assert kw.is_active is False

    # Result should have apply_id
    assert "apply_id" in result
    assert result["dry_run"] is False

    # Apply store should have record
    assert len(apply_store.apply_history) == 1
    assert apply_store.apply_history[0]["applied_by"] == "test_user"


def test_apply_merge_full_flow():
    groups = _make_groups_with_actions(
        "Logistics",
        [
            {
                "type": "merge",
                "keyword_id": "shipment",
                "target_keyword_id": "delivery",
                "suggested_terms": ["freight"],
                "reason": "overlap",
            }
        ],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource(
        [
            _kw("shipment", terms=["ship", "send"]),
            _kw("delivery", terms=["deliver", "dropoff"]),
        ]
    )
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-002",
        analysis=analysis,
        request_actions=[AIApplyAction(group_index=0, action_index=0)],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=False,
        refresh_after=False,
        applied_by="admin",
    )

    # Source should be deleted
    assert keyword_source.get_keyword("shipment") is None

    # Target should have merged terms
    delivery = keyword_source.get_keyword("delivery")
    assert delivery is not None
    assert set(delivery.terms) == {"deliver", "dropoff", "ship", "send", "freight"}

    assert result["dry_run"] is False


def test_apply_keep_action_no_mutation():
    groups = _make_groups_with_actions(
        "G",
        [{"type": "keep", "keyword_id": "good_kw", "reason": "fine as is"}],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource([_kw("good_kw")])
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-003",
        analysis=analysis,
        request_actions=[AIApplyAction(group_index=0, action_index=0)],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=False,
        refresh_after=False,
    )

    assert result["dry_run"] is False
    # No mutations for keep
    assert len(result.get("mutations", [])) == 0


def test_apply_skips_missing_keyword():
    groups = _make_groups_with_actions(
        "G",
        [{"type": "deactivate", "keyword_id": "nonexistent_kw", "reason": "old"}],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource([_kw("other_kw")])
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-004",
        analysis=analysis,
        request_actions=[AIApplyAction(group_index=0, action_index=0)],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=False,
        refresh_after=False,
    )

    # Should be in skipped list
    assert len(result["actions_skipped"]) == 1
    assert "not found" in result["actions_skipped"][0].reason


def test_apply_keyword_id_resolution():
    groups = _make_groups_with_actions(
        "G",
        [
            {
                "type": "rename",
                "keyword_id": "my_kw",
                "suggested_label": "Renamed",
                "reason": "clarity",
            }
        ],
    )
    analysis = _make_analysis(groups)

    keyword_source = FakeKeywordSource([_kw("my_kw", label="Original")])
    apply_store = FakeApplyStore()

    result = apply_approved_actions(
        analysis_id="test-005",
        analysis=analysis,
        request_actions=[AIApplyAction(keyword_id="my_kw")],
        keyword_source=keyword_source,
        apply_store=cast(PostgresAiApplyStore, apply_store),
        dry_run=False,
        refresh_after=False,
    )

    kw = keyword_source.get_keyword("my_kw")
    assert kw is not None
    assert kw.label == "Renamed"
