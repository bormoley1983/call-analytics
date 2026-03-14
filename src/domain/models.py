# src/domain/models.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CallMeta:
    source_file: str
    source_path: str
    call_id: str
    direction: str          # "incoming" | "outgoing" | "unknown"
    src_number: str
    dst_number: str
    date: str               # "YYYYMMDD"
    time: str               # "HHMMSS"
    asterisk_uniqueid: str
    manager_name: str
    manager_id: str
    role: str               # "sales" | "management" | "development" | "unknown"
    audio_seconds: float = 0.0
    status: str = "pending" # "transcribed" | "skipped_too_small" | "skipped_too_short"


@dataclass
class Segment:
    start: float
    end: float
    text: str


@dataclass
class Transcript:
    call_id: str
    text: str
    text_uk: str
    language: str
    segments: List[Segment] = field(default_factory=list)
    segments_uk: List[Segment] = field(default_factory=list)
    manager_name: str = ""
    manager_id: str = ""
    role: str = ""
    translation_error: Optional[str] = None


@dataclass
class Analysis:
    call_id: str
    spam_probability: float
    effective_call: bool
    intent: str             # from VALID_INTENTS_UK
    direction: str
    outcome: str            # from VALID_OUTCOMES_UK
    summary: str
    key_questions: List[str] = field(default_factory=list)
    objections: List[str] = field(default_factory=list)
    manager_name: str = ""
    manager_id: str = ""
    role: str = ""
    analysis_error: Optional[str] = None


@dataclass
class CallResult:
    meta: CallMeta
    status: str             # "processed" | "skipped_too_small" | "skipped_too_short"
    analysis: Optional[Analysis] = None
    transcript: Optional[Transcript] = None