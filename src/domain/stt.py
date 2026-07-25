from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SttRequest:
    call_id: str
    audio_path: Path
    audio_seconds: float
    audio_sha256: str
    language: str = "auto"


@dataclass(frozen=True)
class SttSegment:
    start: float
    end: float
    text: str


@dataclass(frozen=True)
class SttWord:
    start: float
    end: float
    text: str
    confidence: float | None = None


@dataclass(frozen=True)
class SttIdentity:
    provider: str
    model_id: str
    model_revision: str
    config_hash: str


@dataclass(frozen=True)
class SttResult:
    call_id: str
    language: str
    duration: float
    segments: list[SttSegment]
    raw_text: str
    warnings: list[str] = field(default_factory=list)
    words: list[SttWord] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class SttFailure:
    call_id: str
    category: str
    retryable: bool
    detail: str
    meta: dict[str, Any] = field(default_factory=dict)
