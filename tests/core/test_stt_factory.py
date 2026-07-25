from types import SimpleNamespace

import pytest

from adapters.stt_canary import CanarySttAdapter
from adapters.stt_faster_whisper import FasterWhisperSttAdapter
from core.stt_factory import build_stt_adapter


def _base_config():
    return dict(
        stt_engine="faster-whisper",
        whisper_model="large-v3-turbo",
        whisper_device="cpu",
        whisper_compute_type="float32",
        whisper_beam_size=5,
        whisper_initial_prompt="prompt",
        canary_model_id="nvidia/canary-1b-v2",
        canary_model_revision="unknown",
        canary_compute_type="float16",
        canary_batch_size=1,
        canary_beam_size=1,
    )


def test_build_stt_adapter_returns_faster_whisper():
    cfg = SimpleNamespace(**_base_config())

    adapter = build_stt_adapter(cfg)

    assert isinstance(adapter, FasterWhisperSttAdapter)


def test_build_stt_adapter_returns_canary():
    payload = _base_config()
    payload["stt_engine"] = "canary"
    cfg = SimpleNamespace(**payload)

    adapter = build_stt_adapter(cfg)

    assert isinstance(adapter, CanarySttAdapter)


def test_build_stt_adapter_rejects_unknown_engine():
    payload = _base_config()
    payload["stt_engine"] = "unknown"
    cfg = SimpleNamespace(**payload)

    with pytest.raises(ValueError):
        build_stt_adapter(cfg)
