"""STT adapter factory (composition-root helper).

Selects the concrete STT adapter based on ``config.stt_engine``. Lives in
``adapters`` (not ``core``) because it imports concrete adapters — only
composition roots (``api/runner.py``, ``cli.py``, ``stt_replay.py``) call it.
"""

from __future__ import annotations

from adapters.stt_canary import CanarySttAdapter
from adapters.stt_faster_whisper import FasterWhisperSttAdapter
from domain.config import AppConfig
from ports.stt import SttProcessorPort


def build_stt_adapter(config: AppConfig) -> SttProcessorPort:
    engine = getattr(config, "stt_engine", "faster-whisper").strip().lower()
    if engine == "faster-whisper":
        return FasterWhisperSttAdapter(config)
    if engine == "canary":
        return CanarySttAdapter(config)
    raise ValueError(f"Unsupported STT engine: {engine}")
