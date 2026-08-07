from __future__ import annotations

import gc
import hashlib
import importlib
import inspect
import logging
from collections.abc import Iterator, Sequence

from domain.config import AppConfig
from domain.stt import SttFailure, SttIdentity, SttRequest, SttResult, SttSegment
from ports.stt import SttProcessorPort

logger = logging.getLogger(__name__)


class CanarySttAdapter(SttProcessorPort):
    """NVIDIA Canary adapter using NeMo ASR runtime.

    The adapter keeps vendor-specific runtime handling fully isolated behind
    the SttProcessorPort contract.
    """

    def __init__(self, config: AppConfig):
        self._config = config
        self._model = None
        self._model_load_error: str | None = None
        cfg_material = "|".join(
            [
                config.stt_engine,
                config.canary_model_id,
                config.canary_model_revision,
                getattr(config, "canary_device", "cpu"),
                config.canary_compute_type,
                str(config.canary_batch_size),
                str(config.canary_beam_size),
            ]
        )
        self._identity = SttIdentity(
            provider="canary",
            model_id=config.canary_model_id,
            model_revision=config.canary_model_revision,
            config_hash=hashlib.sha256(cfg_material.encode("utf-8")).hexdigest()[:16],
        )

    @property
    def identity(self) -> SttIdentity:
        return self._identity

    def _get_model(self):
        if self._model is not None:
            return self._model
        if self._model_load_error is not None:
            raise RuntimeError(self._model_load_error)

        try:
            torch = importlib.import_module("torch")
            nemo_models = importlib.import_module("nemo.collections.asr.models")
            EncDecMultiTaskModel = nemo_models.EncDecMultiTaskModel

            model = EncDecMultiTaskModel.from_pretrained(
                model_name=self._config.canary_model_id
            )
            device = getattr(self._config, "canary_device", "cpu")
            if device.startswith("cuda") and not torch.cuda.is_available():
                logger.warning(
                    "CANARY_DEVICE requests CUDA but CUDA is unavailable; falling back to CPU"
                )
                device = "cpu"
            model = model.to(device)
            model.eval()
            self._model = model
            return model
        except Exception as exc:
            self._model_load_error = repr(exc)
            raise

    @staticmethod
    def _extract_text(output) -> str:
        if isinstance(output, str):
            return output.strip()
        if hasattr(output, "text"):
            return str(output.text or "").strip()
        if isinstance(output, dict):
            for key in ("text", "pred_text", "transcript"):
                if output.get(key):
                    return str(output[key]).strip()
        return str(output).strip()

    @staticmethod
    def _extract_segments(output, audio_seconds: float) -> list[SttSegment]:
        segments: list[SttSegment] = []

        def _append(start, end, text) -> None:
            t = str(text or "").strip()
            if not t:
                return
            try:
                s = float(start)
            except (ValueError, TypeError):
                s = 0.0
            try:
                e = float(end)
            except (ValueError, TypeError):
                e = float(audio_seconds)
            e = max(e, s)
            segments.append(SttSegment(start=s, end=e, text=t))

        # Common dict layout: {"segments": [{"start", "end", "text"}, ...]}
        if isinstance(output, dict) and isinstance(output.get("segments"), list):
            for seg in output["segments"]:
                if isinstance(seg, dict):
                    _append(
                        seg.get("start", 0.0),
                        seg.get("end", audio_seconds),
                        seg.get("text", ""),
                    )

        # Object layout with .segments attribute
        obj_segments = getattr(output, "segments", None)
        if isinstance(obj_segments, list):
            for seg in obj_segments:
                if isinstance(seg, dict):
                    _append(
                        seg.get("start", 0.0),
                        seg.get("end", audio_seconds),
                        seg.get("text", ""),
                    )
                else:
                    _append(
                        getattr(seg, "start", 0.0),
                        getattr(seg, "end", audio_seconds),
                        getattr(seg, "text", ""),
                    )

        # Fallback: one full-call segment from extracted text
        if not segments:
            text = CanarySttAdapter._extract_text(output)
            if text:
                segments.append(
                    SttSegment(start=0.0, end=float(audio_seconds), text=text)
                )

        return segments

    def _transcribe_kwargs(self, request: SttRequest) -> dict:
        def _norm_lang(value: str) -> str | None:
            normalized = (value or "").strip().lower()
            if normalized in {"", "auto", "mixed", "multilingual", "uk+ru", "ru+uk"}:
                return None
            return value

        source_lang = _norm_lang(
            getattr(self._config, "canary_source_lang", request.language)
        )
        target_lang = _norm_lang(
            getattr(self._config, "canary_target_lang", request.language)
        )

        kwargs = {
            "batch_size": getattr(self._config, "canary_batch_size", 1),
            "beam_size": getattr(self._config, "canary_beam_size", 1),
            "task": getattr(self._config, "canary_task", "asr"),
            "return_hypotheses": getattr(
                self._config, "canary_return_hypotheses", True
            ),
        }
        if source_lang is not None:
            kwargs["source_lang"] = source_lang
        if target_lang is not None:
            kwargs["target_lang"] = target_lang
        return kwargs

    @staticmethod
    def _filter_supported_kwargs(fn, kwargs: dict) -> dict:
        try:
            params = inspect.signature(fn).parameters
        except (TypeError, ValueError):
            return kwargs
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        return {k: v for k, v in kwargs.items() if k in params}

    def transcribe_many(
        self, requests: Sequence[SttRequest]
    ) -> Iterator[SttResult | SttFailure]:
        try:
            model = self._get_model()
        except (RuntimeError, ImportError, OSError) as exc:
            for request in requests:
                yield SttFailure(
                    call_id=request.call_id,
                    category="model_init_error",
                    retryable=False,
                    detail=repr(exc),
                    meta={
                        "provider": self.identity.provider,
                        "model_id": self.identity.model_id,
                    },
                )
            return

        for request in requests:
            try:
                kwargs = self._filter_supported_kwargs(
                    model.transcribe, self._transcribe_kwargs(request)
                )
                outputs = model.transcribe([str(request.audio_path)], **kwargs)
                if isinstance(outputs, (list, tuple)) and outputs:
                    output = outputs[0]
                else:
                    output = outputs

                segments = self._extract_segments(output, request.audio_seconds)
                text = "\n".join(seg.text for seg in segments).strip()

                yield SttResult(
                    call_id=request.call_id,
                    language=request.language,
                    duration=float(request.audio_seconds),
                    segments=segments,
                    raw_text=text,
                    warnings=[],
                    timings={
                        "batch_size": float(
                            getattr(self._config, "canary_batch_size", 1)
                        )
                    },
                )
            except (OSError, RuntimeError, ValueError) as exc:
                yield SttFailure(
                    call_id=request.call_id,
                    category="transcription_error",
                    retryable=False,
                    detail=repr(exc),
                    meta={
                        "provider": self.identity.provider,
                        "model_id": self.identity.model_id,
                        "audio_path": str(request.audio_path),
                    },
                )

    def close(self) -> None:
        model = self._model
        self._model = None

        if model is not None:
            del model

        gc.collect()

        try:
            torch = importlib.import_module("torch")

            if torch.cuda.is_available():
                # Return cached allocator blocks to the driver so other
                # GPU processes (for example Ollama) can allocate VRAM.
                torch.cuda.empty_cache()
                torch.cuda.ipc_collect()
        except (ImportError, RuntimeError):
            # Best-effort cleanup only; never fail pipeline shutdown.
            pass
