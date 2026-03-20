from __future__ import annotations

from typing import Any, Iterable, Protocol

from domain.keywords import KeywordDefinition


class KeywordSource(Protocol):
    source_name: str

    def list_keywords(self) -> Iterable[KeywordDefinition]: ...

    def close(self) -> None: ...


class KeywordCatalogStore(Protocol):
    def list_keywords(self) -> Iterable[KeywordDefinition]: ...
    def upsert_keyword(self, keyword: KeywordDefinition) -> KeywordDefinition: ...
    def delete_keyword(self, keyword_id: str) -> bool: ...
    def close(self) -> None: ...


class KeywordMatchStore(Protocol):
    def replace_call_keyword_matches(self, call_id: str, rows: list[dict[str, Any]]) -> None: ...


class MaterializationStateStore(Protocol):
    def mark_materialization_completed(self, processed_calls: int, matched_calls: int, stored_rows: int) -> None: ...


class RefreshableKeywordStore(KeywordSource, KeywordCatalogStore, KeywordMatchStore, MaterializationStateStore, Protocol):
    pass
