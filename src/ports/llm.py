from __future__ import annotations

from typing import Any, Dict, List, Protocol


class LlmPort(Protocol):
    def translate_segments_to_uk(
        self, segments: List[Dict[str, Any]]
    ) -> List[str] | None: ...
    def analyze(
        self, call_meta: Dict[str, Any], transcript_text_uk: str
    ) -> Dict[str, Any]: ...
    def analyze_keyword_catalog(
        self, analysis_payload: Dict[str, Any], max_groups: int = 20
    ) -> Dict[str, Any]: ...
    def enrich_candidates(
        self, candidates: List[Dict[str, Any]], max_aliases: int = 3
    ) -> Dict[str, Any]: ...
    def expand_aliases(
        self,
        *,
        keyword_id: str,
        label: str,
        current_terms: List[str],
        evidence_texts: List[str],
        max_aliases: int,
    ) -> Dict[str, Any]: ...
    def generate_deep_insights(
        self, insight_type: str, analysis_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]: ...
