from __future__ import annotations

from typing import Any, Protocol


class KeywordAiAnalysisStorePort(Protocol):
    """Structural protocol for keyword AI analysis history stores."""

    def save_analysis(
        self,
        *,
        request_data: dict[str, Any],
        analysis_input: dict[str, Any],
        ai_analysis: dict[str, Any],
        keyword_source: str,
        reporting_source: str | None,
        ai_model: str | None,
    ) -> dict[str, Any]: ...

    def list_analyses(self, limit: int = 50) -> list[dict[str, Any]]: ...

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None: ...
