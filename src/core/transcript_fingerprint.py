from __future__ import annotations

from hashlib import sha256
from typing import Any


def transcript_text_for_analysis(transcript: dict[str, Any]) -> str:
    return (transcript.get("text_uk") or transcript.get("text") or "").strip()


def transcript_text_sha256(transcript: dict[str, Any]) -> str:
    text = transcript_text_for_analysis(transcript)
    return sha256(text.encode("utf-8")).hexdigest()
