from __future__ import annotations

from typing import Any, Protocol


class DeepInsightsStorePort(Protocol):
    """Structural protocol for deep insights run stores."""

    def create_run(
        self,
        run_id: str,
        *,
        ai_model: str | None = None,
        insight_types: list[str] | None = None,
        request_data: dict[str, Any] | None = None,
    ) -> str: ...

    def add_insights(self, run_id: str, insights: list[dict[str, Any]]) -> int: ...

    def get_run(self, run_id: str) -> dict[str, Any] | None: ...
