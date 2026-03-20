from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_MATCH_FIELDS = ["summary", "key_questions", "objections"]


@dataclass(frozen=True)
class KeywordDefinition:
    keyword_id: str
    label: str
    category: str
    terms: list[str] = field(default_factory=list)
    match_fields: list[str] = field(default_factory=lambda: list(DEFAULT_MATCH_FIELDS))
    is_active: bool = True
