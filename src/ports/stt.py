from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Protocol

from domain.stt import SttFailure, SttIdentity, SttRequest, SttResult


class SttProcessorPort(Protocol):
    @property
    def identity(self) -> SttIdentity: ...

    def transcribe_many(self, requests: Sequence[SttRequest]) -> Iterator[SttResult | SttFailure]: ...

    def close(self) -> None: ...
