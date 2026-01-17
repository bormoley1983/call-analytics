from __future__ import annotations

from typing import Any, Dict, List, Protocol


class LlmPort(Protocol):
    def translate_segments_to_uk(self, segments: List[Dict[str, Any]]) -> List[str] | None: ...
    def analyze(self, call_meta: Dict[str, Any], transcript_text_uk: str) -> Dict[str, Any]: ...