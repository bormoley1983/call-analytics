from __future__ import annotations

from typing import Any, Dict, Protocol


class PbxPort(Protocol):
    def parse_filename(self, name: str) -> Dict[str, Any]: ...