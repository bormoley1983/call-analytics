from __future__ import annotations

from typing import Any, Protocol


class PbxPort(Protocol):
    def parse_filename(self, name: str) -> dict[str, Any]: ...