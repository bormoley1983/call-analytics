from __future__ import annotations

from typing import Iterable, Protocol

from domain.keywords import KeywordDefinition


class KeywordSource(Protocol):
    source_name: str

    def list_keywords(self) -> Iterable[KeywordDefinition]: ...

    def close(self) -> None: ...
