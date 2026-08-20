"""Port for persisting AI apply records."""

from __future__ import annotations

from typing import Any, Protocol


class AiApplyStorePort(Protocol):
    """Persistence boundary for AI apply operations."""

    def apply_actions(
        self,
        analysis_id: str,
        applied_by: str | None,
        dry_run: bool,
        actions_applied: list[dict[str, Any]],
        actions_skipped: list[dict[str, Any]],
        mutations: list[dict[str, Any]],
        keyword_refreshed: bool = False,
        follow_up_ran: bool = False,
        error: str | None = None,
    ) -> str: ...

    def get_apply_history(
        self,
        analysis_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]: ...
