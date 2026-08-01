# -*- coding: utf-8 -*-
"""Core logic for applying AI-suggested keyword catalog mutations.

This module resolves actions from a persisted analysis, validates them against
the live keyword catalog, performs the mutations (or previews in dry-run mode),
and persists the apply record via PostgresAiApplyStore.

Action types supported:
- keep: no-op, just records approval
- merge: merge source keyword into target keyword (delete source)
- rename: update label and optionally terms on a keyword
- expand_aliases: add new alias terms to an existing keyword
- deactivate: set is_active=False on a keyword

Safety rules:
- Merging into a non-existent target is skipped.
- Renaming/expanding/deactivating a non-existent keyword is skipped.
- Self-merge (keyword_id == target_keyword_id) is skipped.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from adapters.ai_apply_postgres import PostgresAiApplyStore
from api.schemas import AIApplyAction, AIMutation, AISkippedAction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Action resolution — map request actions to analysis items
# ---------------------------------------------------------------------------


def _resolve_actions_from_analysis(
    analysis: Dict[str, Any],
    request_actions: List[AIApplyAction],
) -> List[Dict[str, Any]]:
    """Resolve AIApplyActions (by indices or keyword_id) to concrete action dicts.

    The analysis dict has an "items" key with grouped items by item_type.
    Actions are stored under items["action"] with item_key="group_index:action_index".
    Each action item has a "data" dict with the action payload and a "group" dict.

    Alternatively, an action can reference a keyword directly via keyword_id.
    In that case we search groups for matching actions.
    """
    items = analysis.get("items", {})
    stored_actions: List[Dict[str, Any]] = items.get("action", [])
    ai_analysis = analysis.get("ai_analysis", {})
    groups: List[Dict[str, Any]] = ai_analysis.get("groups", [])

    resolved: List[Dict[str, Any]] = []

    for req_action in request_actions:
        if req_action.keyword_id and (
            req_action.group_index is None or req_action.action_index is None
        ):
            # Direct keyword_id reference — find the action by searching groups
            found = _find_action_by_keyword_id(groups, req_action.keyword_id)
            if found:
                resolved.append(found)
            else:
                resolved.append(
                    {
                        "keyword_id": req_action.keyword_id,
                        "_resolve_error": f"Could not resolve action for keyword_id={req_action.keyword_id}",
                    }
                )
        elif req_action.group_index is not None and req_action.action_index is not None:
            # Index-based resolution
            target_key = f"{req_action.group_index}:{req_action.action_index}"
            found = None
            for sa in stored_actions:
                if sa.get("item_key") == target_key:
                    found = sa.get("data", {})
                    break
            if found is None:
                # Fallback: try to find from groups directly
                found = _find_action_from_groups(
                    groups, req_action.group_index, req_action.action_index
                )
            if found:
                resolved.append(found)
            else:
                resolved.append(
                    {
                        "_resolve_error": f"Action at group_index={req_action.group_index}, action_index={req_action.action_index} not found",
                    }
                )
        else:
            resolved.append(
                {
                    "_resolve_error": "Invalid action reference (need indices or keyword_id)",
                }
            )

    return resolved


def _find_action_from_groups(
    groups: List[Dict[str, Any]], group_index: int, action_index: int
) -> Optional[Dict[str, Any]]:
    """Find an action from the ai_analysis.groups structure."""
    if group_index < 0 or group_index >= len(groups):
        return None
    group = groups[group_index]
    actions = group.get("suggested_actions", [])
    if action_index < 0 or action_index >= len(actions):
        return None
    return dict(actions[action_index])


def _find_action_by_keyword_id(
    groups: List[Dict[str, Any]], keyword_id: str
) -> Optional[Dict[str, Any]]:
    """Find the first non-'keep' action that references this keyword_id."""
    for group in groups:
        for action in group.get("suggested_actions", []):
            if action.get("keyword_id") == keyword_id and action.get("type") != "keep":
                return dict(action)
    return None


# ---------------------------------------------------------------------------
# Validation & mutation building
# ---------------------------------------------------------------------------


def _validate_and_build_mutations(
    resolved_actions: List[Dict[str, Any]],
    keyword_ids: Set[str],
    keywords_by_id: Dict[str, Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[AISkippedAction], List[AIMutation]]:
    """Validate resolved actions against the live catalog and build mutations.

    Returns (applied_actions, skipped_actions, mutations).
    """
    applied: List[Dict[str, Any]] = []
    skipped: List[AISkippedAction] = []
    mutations: List[AIMutation] = []

    for action in resolved_actions:
        if "_resolve_error" in action:
            skipped.append(
                AISkippedAction(action=action, reason=action["_resolve_error"])
            )
            continue

        action_type = action.get("type", "")
        kw_id = action.get("keyword_id", "")

        # Safety: skip if keyword doesn't exist in catalog
        if kw_id and kw_id not in keyword_ids:
            skipped.append(
                AISkippedAction(
                    action=action,
                    reason=f"Keyword '{kw_id}' not found in catalog",
                )
            )
            continue

        if action_type == "keep":
            applied.append(action)
            # No mutation needed for keep
            continue

        if action_type == "merge":
            target_id = action.get("target_keyword_id", "")
            if not target_id:
                skipped.append(
                    AISkippedAction(
                        action=action, reason="Merge action missing target_keyword_id"
                    )
                )
                continue
            if target_id not in keyword_ids:
                skipped.append(
                    AISkippedAction(
                        action=action,
                        reason=f"Merge target '{target_id}' not found in catalog",
                    )
                )
                continue
            if kw_id == target_id:
                skipped.append(
                    AISkippedAction(
                        action=action,
                        reason=f"Self-merge of '{kw_id}' is not allowed",
                    )
                )
                continue
            applied.append(action)
            mutations.append(
                AIMutation(
                    action_type="merge",
                    keyword_id=kw_id,
                    detail={
                        "target_keyword_id": target_id,
                        "suggested_terms": action.get("suggested_terms", []),
                        "reason": action.get("reason", ""),
                    },
                )
            )

        elif action_type == "rename":
            applied.append(action)
            mutations.append(
                AIMutation(
                    action_type="rename",
                    keyword_id=kw_id,
                    detail={
                        "suggested_label": action.get("suggested_label", ""),
                        "suggested_terms": action.get("suggested_terms", []),
                        "reason": action.get("reason", ""),
                    },
                )
            )

        elif action_type == "expand_aliases":
            applied.append(action)
            mutations.append(
                AIMutation(
                    action_type="expand_aliases",
                    keyword_id=kw_id,
                    detail={
                        "suggested_terms": action.get("suggested_terms", []),
                        "reason": action.get("reason", ""),
                    },
                )
            )

        elif action_type == "deactivate":
            applied.append(action)
            mutations.append(
                AIMutation(
                    action_type="deactivate",
                    keyword_id=kw_id,
                    detail={"reason": action.get("reason", "")},
                )
            )

        else:
            skipped.append(
                AISkippedAction(
                    action=action,
                    reason=f"Unknown action type '{action_type}'",
                )
            )

    return applied, skipped, mutations


# ---------------------------------------------------------------------------
# Keyword mutation execution
# ---------------------------------------------------------------------------


def _execute_mutations_on_keyword_source(
    mutations: List[AIMutation],
    keyword_source: Any,
    dry_run: bool,
) -> None:
    """Execute mutations on the keyword source.

    In dry-run mode, this is a no-op (mutations are just previewed).
    """
    if dry_run:
        return

    for mutation in mutations:
        action_type = mutation.action_type
        kw_id = mutation.keyword_id
        detail = mutation.detail

        if action_type == "merge":
            _do_merge(keyword_source, kw_id, detail)
        elif action_type == "rename":
            _do_rename(keyword_source, kw_id, detail)
        elif action_type == "expand_aliases":
            _do_expand_aliases(keyword_source, kw_id, detail)
        elif action_type == "deactivate":
            _do_deactivate(keyword_source, kw_id)


def _do_merge(keyword_source: Any, keyword_id: str, detail: Dict[str, Any]) -> None:
    """Merge source keyword into target: add source terms to target, delete source."""
    target_id = detail["target_keyword_id"]
    suggested_terms = detail.get("suggested_terms", [])

    # Get existing keywords
    source_kw = keyword_source.get_keyword(keyword_id)
    target_kw = keyword_source.get_keyword(target_id)

    if source_kw is None or target_kw is None:
        logger.warning(
            "Merge skipped: source=%s target=%s (one not found)", keyword_id, target_id
        )
        return

    # Merge terms: combine target terms with source terms and suggested terms
    merged_terms: List[str] = list(target_kw.terms) if target_kw.terms else []
    new_terms = set(merged_terms)

    # Add source terms
    if source_kw.terms:
        for term in source_kw.terms:
            if term not in new_terms:
                merged_terms.append(term)
                new_terms.add(term)

    # Add suggested terms
    for term in suggested_terms:
        if term not in new_terms:
            merged_terms.append(term)
            new_terms.add(term)

    # Update target with merged terms
    from domain.keywords import KeywordDefinition

    updated_target = KeywordDefinition(
        keyword_id=target_kw.keyword_id,
        label=target_kw.label or (detail.get("suggested_label") or ""),
        category=target_kw.category,
        terms=merged_terms,
        match_fields=target_kw.match_fields,
        is_active=target_kw.is_active,
    )
    keyword_source.upsert_keyword(updated_target)

    # Delete source keyword
    keyword_source.delete_keyword(keyword_id)
    logger.info("Merged keyword %s into %s", keyword_id, target_id)


def _do_rename(keyword_source: Any, keyword_id: str, detail: Dict[str, Any]) -> None:
    """Rename a keyword's label and optionally update terms."""
    kw = keyword_source.get_keyword(keyword_id)
    if kw is None:
        logger.warning("Rename skipped: keyword %s not found", keyword_id)
        return

    from domain.keywords import KeywordDefinition

    new_label = detail.get("suggested_label") or kw.label
    new_terms = detail.get("suggested_terms") or kw.terms

    updated = KeywordDefinition(
        keyword_id=kw.keyword_id,
        label=new_label,
        category=kw.category,
        terms=new_terms,
        match_fields=kw.match_fields,
        is_active=kw.is_active,
    )
    keyword_source.upsert_keyword(updated)
    logger.info("Renamed keyword %s to '%s'", keyword_id, new_label)


def _do_expand_aliases(
    keyword_source: Any, keyword_id: str, detail: Dict[str, Any]
) -> None:
    """Add new alias terms to an existing keyword."""
    kw = keyword_source.get_keyword(keyword_id)
    if kw is None:
        logger.warning("Expand aliases skipped: keyword %s not found", keyword_id)
        return

    from domain.keywords import KeywordDefinition

    suggested_terms = detail.get("suggested_terms", [])
    current_terms = list(kw.terms) if kw.terms else []
    current_set = set(current_terms)

    for term in suggested_terms:
        if term not in current_set:
            current_terms.append(term)
            current_set.add(term)

    updated = KeywordDefinition(
        keyword_id=kw.keyword_id,
        label=kw.label,
        category=kw.category,
        terms=current_terms,
        match_fields=kw.match_fields,
        is_active=kw.is_active,
    )
    keyword_source.upsert_keyword(updated)
    logger.info(
        "Expanded aliases for keyword %s: added %d terms",
        keyword_id,
        len(suggested_terms),
    )


def _do_deactivate(keyword_source: Any, keyword_id: str) -> None:
    """Deactivate a keyword (set is_active=False)."""
    kw = keyword_source.get_keyword(keyword_id)
    if kw is None:
        logger.warning("Deactivate skipped: keyword %s not found", keyword_id)
        return

    from domain.keywords import KeywordDefinition

    updated = KeywordDefinition(
        keyword_id=kw.keyword_id,
        label=kw.label,
        category=kw.category,
        terms=kw.terms,
        match_fields=kw.match_fields,
        is_active=False,
    )
    keyword_source.upsert_keyword(updated)
    logger.info("Deactivated keyword %s", keyword_id)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def apply_approved_actions(
    *,
    analysis_id: str,
    analysis: Dict[str, Any],
    request_actions: List[AIApplyAction],
    keyword_source: Any,
    apply_store: PostgresAiApplyStore,
    dry_run: bool = False,
    refresh_after: bool = True,
    applied_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Orchestrate the full apply flow.

    1. Resolve request actions against the analysis.
    2. Validate and build mutations.
    3. Execute mutations (skip if dry_run).
    4. Optionally refresh keyword materialization.
    5. Persist the apply record.

    Returns a dict suitable for AIApplyResult or AIApplyDryRunResult.
    """
    # Build keyword id set from live catalog
    all_keywords = list(keyword_source.list_keywords())
    keyword_ids: Set[str] = {kw.keyword_id for kw in all_keywords}
    keywords_by_id: Dict[str, Dict[str, Any]] = {
        kw.keyword_id: {
            "keyword_id": kw.keyword_id,
            "label": kw.label,
            "category": kw.category,
            "terms": list(kw.terms) if kw.terms else [],
            "is_active": kw.is_active,
        }
        for kw in all_keywords
    }

    # Step 1: Resolve actions
    resolved_actions = _resolve_actions_from_analysis(analysis, request_actions)

    # Step 2: Validate and build mutations
    applied_actions, skipped_actions, mutations = _validate_and_build_mutations(
        resolved_actions, keyword_ids, keywords_by_id
    )

    # Step 3: Execute mutations (no-op if dry_run)
    _execute_mutations_on_keyword_source(mutations, keyword_source, dry_run=dry_run)

    keyword_refreshed = False

    # Step 4: Refresh materialization (only in live mode)
    if not dry_run and refresh_after:
        try:
            from core.keywords_materialize import materialize_call_keywords

            reporting_source = _get_reporting_source_for_refresh()
            if reporting_source is not None:
                materialize_call_keywords(
                    reporting_source=reporting_source,
                    keyword_source=keyword_source,
                    keyword_store=keyword_source,
                    state_store=keyword_source,
                )
                keyword_refreshed = True
                reporting_source.close()
        except Exception:
            logger.exception("Keyword refresh after AI apply failed")

    # Step 5: Persist apply record
    actions_applied_dicts = []
    for a in applied_actions:
        actions_applied_dicts.append(
            {k: v for k, v in a.items() if not k.startswith("_")}
        )

    actions_skipped_dicts = [
        {
            "action": (
                sa.action.model_dump()
                if hasattr(sa.action, "model_dump")
                else sa.action
            ),
            "reason": sa.reason,
        }
        for sa in skipped_actions
    ]

    mutations_dicts = [m.model_dump() for m in mutations]

    apply_id = apply_store.apply_actions(
        analysis_id=analysis_id,
        applied_by=applied_by,
        dry_run=dry_run,
        actions_applied=actions_applied_dicts,
        actions_skipped=actions_skipped_dicts,
        mutations=mutations_dicts,
        keyword_refreshed=keyword_refreshed,
        follow_up_ran=False,
    )

    return {
        "apply_id": apply_id,
        "analysis_id": analysis_id,
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "actions_applied": actions_applied_dicts,
        "actions_skipped": skipped_actions,
        "mutations": mutations,
        "keyword_refreshed": keyword_refreshed,
        "follow_up_ran": False,
    }


def _get_reporting_source_for_refresh() -> Any | None:
    """Get reporting source for keyword materialization refresh."""
    import os

    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        from adapters.reporting_postgres import PostgresReportingSource

        return PostgresReportingSource(dsn)
    return None


def get_apply_history(
    analysis_id: str,
    apply_store: PostgresAiApplyStore,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Get apply history for an analysis."""
    return apply_store.get_apply_history(analysis_id, limit=limit, offset=offset)
