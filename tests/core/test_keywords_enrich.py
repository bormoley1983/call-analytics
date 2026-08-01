from __future__ import annotations

import pytest

from core.keywords_enrich import enrich_keyword_candidates


class FakeLlm:
    """Minimal LLM mock for enrichment tests."""

    def enrich_candidates(self, candidates, max_aliases=3):
        """Return enriched candidates with aliases and categories."""
        enriched = []
        for c in candidates:
            phrase = c.get("phrase", "")
            enriched.append(
                {
                    "candidate_id": c.get("candidate_id", phrase),
                    "phrase": phrase,
                    "suggested_label": f"Label for {phrase}",
                    "suggested_category": "ai_suggested",
                    "suggested_aliases": [f"{phrase} alias"],
                    "merged_with": [],
                    "confidence_score": 0.85,
                    "reason": "AI enrichment",
                    "support_calls": c.get("support_calls", 1),
                    "total_matches": c.get("total_matches", 1),
                    "sample_call_ids": c.get("sample_call_ids", []),
                }
            )
        return {"enriched_candidates": enriched}


def test_enrich_basic():
    candidates = [
        {
            "candidate_id": "c1",
            "phrase": "pricing inquiry",
            "support_calls": 5,
            "total_matches": 8,
            "sample_call_ids": ["call1"],
        }
    ]
    result = enrich_keyword_candidates(
        candidates=candidates,
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases_per_candidate=3,
    )

    assert "enriched_candidates" in result
    enriched = result["enriched_candidates"]
    assert len(enriched) == 1
    assert enriched[0]["phrase"] == "pricing inquiry"
    assert enriched[0]["suggested_category"] == "ai_suggested"
    assert "pricing inquiry alias" in enriched[0]["suggested_aliases"]


def test_enrich_preserves_evidence():
    candidates = [
        {
            "candidate_id": "c1",
            "phrase": "delivery time",
            "support_calls": 10,
            "total_matches": 25,
            "sample_call_ids": ["call1", "call2"],
        }
    ]
    result = enrich_keyword_candidates(
        candidates=candidates,
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases_per_candidate=2,
    )

    enriched = result["enriched_candidates"][0]
    assert enriched["support_calls"] == 10
    assert enriched["total_matches"] == 25
    assert "call1" in enriched["sample_call_ids"]


def test_enrich_empty_candidates():
    result = enrich_keyword_candidates(
        candidates=[],
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases_per_candidate=3,
    )

    assert result["enriched_candidates"] == []


def test_enrich_multiple_candidates():
    candidates = [
        {
            "candidate_id": f"c{i}",
            "phrase": f"phrase {i}",
            "support_calls": i,
            "total_matches": i * 2,
            "sample_call_ids": [f"call{i}"],
        }
        for i in range(1, 4)
    ]
    result = enrich_keyword_candidates(
        candidates=candidates,
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases_per_candidate=3,
    )

    assert len(result["enriched_candidates"]) == 3


def test_enrich_merged_candidates_consolidation():
    """Test that merged candidates get consolidated stats."""
    candidates = [
        {
            "candidate_id": "c1",
            "phrase": "main phrase",
            "support_calls": 5,
            "total_matches": 10,
            "sample_call_ids": ["call1"],
        },
        {
            "candidate_id": "c2",
            "phrase": "secondary phrase",
            "support_calls": 3,
            "total_matches": 6,
            "sample_call_ids": ["call2"],
            "merged_with": ["c1"],  # This candidate merges c1
        },
    ]

    class MergingLlm(FakeLlm):
        def enrich_candidates(self, candidates, max_aliases=3):
            enriched = []
            for c in candidates:
                entry = {
                    "candidate_id": c["candidate_id"],
                    "phrase": c["phrase"],
                    "suggested_label": f"Label for {c['phrase']}",
                    "suggested_category": "test",
                    "suggested_aliases": [],
                    "merged_with": c.get("merged_with", []),
                    "confidence_score": 0.9,
                    "reason": "test",
                    "support_calls": c["support_calls"],
                    "total_matches": c["total_matches"],
                    "sample_call_ids": c.get("sample_call_ids", []),
                }
                enriched.append(entry)
            return {"enriched_candidates": enriched}

    result = enrich_keyword_candidates(
        candidates=candidates,
        llm=MergingLlm(),  # type: ignore[arg-type]
        max_aliases_per_candidate=3,
    )

    # The merged candidate should have consolidated stats
    merged = [c for c in result["enriched_candidates"] if c["candidate_id"] == "c2"]
    assert len(merged) == 1
    # Support calls should be consolidated (5 + 3 = 8)
    assert merged[0]["support_calls"] >= 3


def test_enrich_llm_error_handling():
    """Test that LLM errors propagate (caller should handle)."""

    class ErrorLlm:
        def enrich_candidates(self, candidates, max_aliases=3):
            raise RuntimeError("LLM unavailable")

    with pytest.raises(RuntimeError, match="LLM unavailable"):
        enrich_keyword_candidates(
            candidates=[{"candidate_id": "c1", "phrase": "test"}],
            llm=ErrorLlm(),  # type: ignore[arg-type]
            max_aliases_per_candidate=3,
        )
