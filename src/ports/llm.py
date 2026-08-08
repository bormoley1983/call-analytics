from __future__ import annotations

from typing import Any, Protocol


class LlmPort(Protocol):
    def translate_segments_to_uk(
        self, segments: list[dict[str, Any]]
    ) -> list[str] | None: ...
    def analyze(
        self, call_meta: dict[str, Any], transcript_text_uk: str
    ) -> dict[str, Any]: ...
    def analyze_keyword_catalog(
        self, analysis_payload: dict[str, Any], max_groups: int = 20
    ) -> dict[str, Any]: ...
    def enrich_candidates(
        self, candidates: list[dict[str, Any]], max_aliases: int = 3
    ) -> dict[str, Any]: ...
    def expand_aliases(
        self,
        *,
        keyword_id: str,
        label: str,
        current_terms: list[str],
        evidence_texts: list[str],
        max_aliases: int,
    ) -> dict[str, Any]: ...
    def generate_deep_insights(
        self, insight_type: str, analysis_records: list[dict[str, Any]]
    ) -> dict[str, Any]: ...
