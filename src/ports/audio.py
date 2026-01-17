from __future__ import annotations

from pathlib import Path
from typing import Protocol


class AudioPort(Protocol):
    def normalize(self, src: Path, dst: Path) -> None: ...
    def duration_seconds(self, path: Path) -> float: ...