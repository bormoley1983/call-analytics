"""Domain models for AI-suggested keyword catalog mutations.

Plain dataclasses shared by core orchestration and the API edge. The API
layer (``api/schemas.py``) builds Pydantic request/response models that
convert to/from these types at the boundary, so core never imports from
the API layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AIApplyAction:
    """Single action to apply, referenced by group/action index or keyword_id."""

    group_index: int | None = None
    action_index: int | None = None
    keyword_id: str | None = None

    def __post_init__(self) -> None:
        has_indices = self.group_index is not None and self.action_index is not None
        has_keyword_id = bool(self.keyword_id)
        if not has_indices and not has_keyword_id:
            raise ValueError(
                "AIApplyAction requires either (group_index + action_index) or keyword_id"
            )


@dataclass(frozen=True)
class AIMutation:
    """Single mutation performed or previewed."""

    action_type: str
    keyword_id: str
    detail: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AISkippedAction:
    """Action that was skipped during apply with reason."""

    action: dict[str, object]
    reason: str
