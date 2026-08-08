from __future__ import annotations

import hashlib
import importlib
import logging
from collections.abc import Iterator, Sequence
from typing import Any

from domain.config import AppConfig
from domain.stt import SttFailure, SttIdentity, SttRequest, SttResult, SttSegment
from ports.stt import SttProcessorPort

logger = logging.getLogger(__name__)


class FasterWhisperSttAdapter(SttProcessorPort):
    def __init__(self, config: AppConfig):
        self._config = config
        self._model: Any | None = None

        cfg_material = "|".join(
            [
                config.whisper_model,
                config.whisper_device,
                config.whisper_compute_type,
                str(config.whisper_beam_size),
                config.whisper_initial_prompt,
            ]
        )
        self._identity = SttIdentity(
            provider="faster_whisper",
            model_id=config.whisper_model,
            model_revision="unknown",
            config_hash=hashlib.sha256(cfg_material.encode("utf-8")).hexdigest()[:16],
        )

    @property
    def identity(self) -> SttIdentity:
        return self._identity

    def _get_model(self):
        if self._model is None:
            from faster_whisper import WhisperModel

            self._model = WhisperModel(
                self._config.whisper_model,
                device=self._config.whisper_device,
                compute_type=self._config.whisper_compute_type,
            )
        return self._model

    def transcribe_many(
        self, requests: Sequence[SttRequest]
    ) -> Iterator[SttResult | SttFailure]:
        model = self._get_model()
        for request in requests:
            try:
                requested_lang = (request.language or "").strip().lower()
                language_arg = (
                    None
                    if requested_lang
                    in {"", "auto", "mixed", "multilingual", "uk+ru", "ru+uk"}
                    else request.language
                )
                segments, info = model.transcribe(
                    str(request.audio_path),
                    language=language_arg,
                    initial_prompt=self._config.whisper_initial_prompt,
                    vad_filter=True,
                    beam_size=self._config.whisper_beam_size,
                    word_timestamps=False,
                )
                mapped_segments: list[SttSegment] = []
                text_parts: list[str] = []
                for seg in segments:
                    text = (seg.text or "").strip()
                    if not text:
                        continue
                    mapped_segments.append(
                        SttSegment(
                            start=float(seg.start), end=float(seg.end), text=text
                        )
                    )
                    text_parts.append(text)

                yield SttResult(
                    call_id=request.call_id,
                    language=getattr(info, "language", request.language),
                    duration=float(getattr(info, "duration", request.audio_seconds)),
                    segments=mapped_segments,
                    raw_text="\n".join(text_parts).strip(),
                )
            except (OSError, RuntimeError, ValueError) as exc:
                yield SttFailure(
                    call_id=request.call_id,
                    category="transcription_error",
                    retryable=False,
                    detail=repr(exc),
                    meta={"audio_path": str(request.audio_path)},
                )

    def close(self) -> None:
        if self._model is not None:
            # Explicitly delete the model reference to help Python's GC
            # reclaim GPU memory, then flush CUDA cache if available.
            del self._model
            self._model = None
            try:
                torch = importlib.import_module("torch")

                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.debug("CUDA cache emptied after model unload")
            except ImportError:
                pass  # torch may not be imported in all environments
