from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import yaml

from domain.keywords import DEFAULT_MATCH_FIELDS, KeywordDefinition


def _normalize_terms(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


class YamlKeywordSource:
    source_name = "yaml"

    def __init__(self, path: Path):
        self.path = path

    def load_keywords(self, strict: bool = False) -> list[KeywordDefinition]:
        if not self.path.exists():
            if strict:
                raise FileNotFoundError(f"Keyword config not found: {self.path}")
            return []

        try:
            payload = yaml.safe_load(self.path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            if strict:
                raise ValueError(f"Invalid keyword YAML: {self.path}") from exc
            return []

        if payload is None:
            payload = []
        if not isinstance(payload, list):
            if strict:
                raise ValueError(f"Keyword config must be a YAML list: {self.path}")
            return []

        keywords: list[KeywordDefinition] = []
        for item in payload:
            if not isinstance(item, dict):
                if strict:
                    raise ValueError(f"Keyword entries must be mappings: {self.path}")
                continue
            keyword_id = str(item.get("keyword_id") or item.get("id") or "").strip()
            label = str(item.get("label") or "").strip()
            if not keyword_id or not label:
                if strict:
                    raise ValueError(f"Keyword entries require keyword_id and label: {self.path}")
                continue
            match_fields = _normalize_terms(item.get("match_fields")) or list(DEFAULT_MATCH_FIELDS)
            keywords.append(
                KeywordDefinition(
                    keyword_id=keyword_id,
                    label=label,
                    category=str(item.get("category") or "general"),
                    terms=_normalize_terms(item.get("terms")),
                    match_fields=match_fields,
                    is_active=bool(item.get("is_active", True)),
                )
            )
        return keywords

    def list_keywords(self) -> Iterable[KeywordDefinition]:
        return self.load_keywords(strict=False)

    def close(self) -> None:
        return None
