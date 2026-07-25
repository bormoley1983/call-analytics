from __future__ import annotations

import time
from typing import Any

from core.rules import correct_brand_names
from domain.config import AppConfig
from domain.stt import SttFailure, SttRequest
from ports.stt import SttProcessorPort


class SttService:
    def __init__(self, processor: SttProcessorPort, config: AppConfig):
        self._processor = processor
        self._config = config

    @property
    def identity(self):
        return self._processor.identity

    def transcribe_one(self, request: SttRequest) -> dict[str, Any]:
        started = time.perf_counter()
        for item in self._processor.transcribe_many([request]):
            if isinstance(item, SttFailure):
                raise RuntimeError(f"STT failed for {item.call_id}: {item.category} {item.detail}")

            corrected_segments: list[dict[str, Any]] = []
            full_text: list[str] = []
            for seg in item.segments:
                text = (seg.text or "").strip()
                if not text:
                    continue
                text = correct_brand_names(text, self._config.brand_corrections)
                corrected_segments.append({"start": float(seg.start), "end": float(seg.end), "text": text})
                full_text.append(text)

            return {
                "language": item.language,
                "duration": float(item.duration),
                "segments": corrected_segments,
                "text": "\n".join(full_text).strip(),
                "_stt": {
                    "provider": self.identity.provider,
                    "model_id": self.identity.model_id,
                    "model_revision": self.identity.model_revision,
                    "config_hash": self.identity.config_hash,
                    "timings": item.timings,
                    "elapsed_seconds": time.perf_counter() - started,
                    "warnings": item.warnings,
                },
            }
        raise RuntimeError(f"STT processor returned no result for {request.call_id}")

    def close(self) -> None:
        self._processor.close()
