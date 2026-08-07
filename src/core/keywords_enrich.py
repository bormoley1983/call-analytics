from __future__ import annotations

import logging
from typing import Any

from ports.llm import LlmPort

logger = logging.getLogger(__name__)


def enrich_keyword_candidates(
    candidates: list[dict[str, Any]],
    llm: LlmPort,
    *,
    max_aliases_per_candidate: int = 3,
    merge_near_duplicates: bool = True,
) -> dict[str, Any]:
    """Enrich keyword candidates using AI.

    Takes raw candidate dicts from generate_keyword_candidates(), sends to LLM for enrichment,
    then merges results back preserving original evidence fields.

    Returns dict matching KeywordGenerationEnrichResult shape.
    """
    if not candidates:
        return {
            "enriched_candidates": [],
            "original_count": 0,
            "enriched_count": 0,
            "merged_count": 0,
        }

    original_count = len(candidates)

    # Build LLM-compatible input (only fields the prompt needs)
    llm_candidates: list[dict[str, Any]] = []
    for c in candidates:
        llm_candidates.append(
            {
                "candidate_id": c.get("candidate_id", ""),
                "phrase": c.get("phrase", ""),
                "support_calls": c.get("support_calls", 0),
                "total_matches": c.get("total_matches", 0),
                "sample_call_ids": c.get("sample_call_ids", []),
            }
        )

    # Call LLM for enrichment
    llm_result = llm.enrich_candidates(
        llm_candidates, max_aliases=max_aliases_per_candidate
    )

    enriched_from_llm = llm_result.get("enriched_candidates", [])
    merge_count = llm_result.get("merge_count", 0)

    # Build a lookup of original candidates by candidate_id for evidence preservation
    original_by_id: dict[str, dict[str, Any]] = {}
    for c in candidates:
        cid = c.get("candidate_id", "")
        if cid:
            original_by_id[cid] = c

    # Map LLM-enriched candidates back with preserved evidence
    enriched_candidates: list[dict[str, Any]] = []
    merged_candidate_ids: set[str] = set()

    for item in enriched_from_llm:
        cid = item.get("candidate_id", "")
        original = original_by_id.get(cid, {})

        # Collect merged candidate IDs
        for merged_id in item.get("merged_with", []):
            merged_candidate_ids.add(merged_id)

        enriched_candidates.append(
            {
                "candidate_id": cid,
                "phrase": item.get("phrase") or original.get("phrase", ""),
                "suggested_label": item.get("suggested_label"),
                "suggested_category": item.get("suggested_category"),
                "suggested_aliases": item.get("suggested_aliases", []),
                "merged_with": item.get("merged_with", []),
                "confidence_score": item.get("confidence_score"),
                "reason": item.get("reason"),
                # Preserve original evidence
                "support_calls": original.get("support_calls", 0),
                "total_matches": original.get("total_matches", 0),
                "sample_call_ids": original.get("sample_call_ids", []),
            }
        )

    # If merging was requested and LLM suggested merges, consolidate merged candidates' stats
    if merge_near_duplicates:
        enriched_candidates = _consolidate_merged_candidates(
            enriched_candidates, original_by_id, merged_candidate_ids
        )

    # Add any candidates that the LLM didn't return (fallback — pass through originals)
    returned_ids = {
        item.get("candidate_id") for item in enriched_from_llm
    } | merged_candidate_ids
    for c in candidates:
        cid = c.get("candidate_id", "")
        if cid and cid not in returned_ids:
            enriched_candidates.append(
                {
                    "candidate_id": cid,
                    "phrase": c.get("phrase", ""),
                    "suggested_label": None,
                    "suggested_category": None,
                    "suggested_aliases": [],
                    "merged_with": [],
                    "confidence_score": None,
                    "reason": None,
                    "support_calls": c.get("support_calls", 0),
                    "total_matches": c.get("total_matches", 0),
                    "sample_call_ids": c.get("sample_call_ids", []),
                }
            )

    enriched_count = len(enriched_candidates)

    logger.info(
        "Enrichment complete: original=%d enriched=%d merged=%d",
        original_count,
        enriched_count,
        merge_count,
    )

    return {
        "enriched_candidates": enriched_candidates,
        "original_count": original_count,
        "enriched_count": enriched_count,
        "merged_count": merge_count,
    }


def _consolidate_merged_candidates(
    enriched: list[dict[str, Any]],
    original_by_id: dict[str, dict[str, Any]],
    merged_ids: set[str],
) -> list[dict[str, Any]]:
    """Consolidate stats for candidates that were merged.

    When candidate B is merged into candidate A, add B's support_calls/total_matches/sample_call_ids
    to A's evidence.
    """
    if not merged_ids:
        return enriched

    # Build lookup by candidate_id
    by_id: dict[str, dict[str, Any]] = {}
    for item in enriched:
        by_id[item["candidate_id"]] = item

    for merged_id in merged_ids:
        original = original_by_id.get(merged_id)
        if not original:
            continue

        # Find which candidate this was merged into
        target_cid = None
        for item in enriched:
            if merged_id in item.get("merged_with", []):
                target_cid = item["candidate_id"]
                break

        if not target_cid or target_cid not in by_id:
            continue

        target = by_id[target_cid]
        # Consolidate stats
        target["support_calls"] += original.get("support_calls", 0)
        target["total_matches"] += original.get("total_matches", 0)

        # Merge sample_call_ids (union, deduplicated)
        existing_calls = set(target.get("sample_call_ids", []))
        new_calls = set(original.get("sample_call_ids", []))
        target["sample_call_ids"] = list(existing_calls | new_calls)

    return enriched
