import sys
from types import SimpleNamespace

from adapters.stt_faster_whisper import FasterWhisperSttAdapter
from domain.stt import SttFailure, SttRequest, SttResult


class _Seg:
    def __init__(self, start, end, text):
        self.start = start
        self.end = end
        self.text = text


class _Info:
    def __init__(self, language, duration):
        self.language = language
        self.duration = duration


class _WhisperModelOk:
    def __init__(self, model, device=None, compute_type=None):
        self.model = model

    def transcribe(self, audio_path, **kwargs):
        return [
            _Seg(0.0, 0.5, " hello "),
            _Seg(0.5, 1.0, ""),
        ], _Info("uk", 1.0)


class _WhisperModelCaptureLang:
    last_language = "__unset__"

    def __init__(self, model, device=None, compute_type=None):
        self.model = model

    def transcribe(self, audio_path, **kwargs):
        _WhisperModelCaptureLang.last_language = kwargs.get("language", "__missing__")
        return [_Seg(0.0, 1.0, "test")], _Info("ru", 1.0)


class _WhisperModelFail:
    def __init__(self, model, device=None, compute_type=None):
        self.model = model

    def transcribe(self, audio_path, **kwargs):
        raise RuntimeError("boom")


def _build_config():
    return SimpleNamespace(
        whisper_model="large-v3-turbo",
        whisper_device="cpu",
        whisper_compute_type="float32",
        whisper_beam_size=5,
        whisper_initial_prompt="prompt",
    )


def test_faster_whisper_adapter_maps_segments(monkeypatch, tmp_path):
    monkeypatch.setitem(
        sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=_WhisperModelOk)
    )

    adapter = FasterWhisperSttAdapter(_build_config())  # type: ignore[arg-type]
    req = SttRequest(
        call_id="c1",
        audio_path=tmp_path / "a.wav",
        audio_seconds=1.0,
        audio_sha256="abc",
    )

    items = list(adapter.transcribe_many([req]))

    assert len(items) == 1
    assert isinstance(items[0], SttResult)
    assert items[0].language == "uk"
    assert items[0].raw_text == "hello"
    assert len(items[0].segments) == 1


def test_faster_whisper_adapter_returns_failure(monkeypatch, tmp_path):
    monkeypatch.setitem(
        sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=_WhisperModelFail)
    )

    adapter = FasterWhisperSttAdapter(_build_config())  # type: ignore[arg-type]
    req = SttRequest(
        call_id="c1",
        audio_path=tmp_path / "a.wav",
        audio_seconds=1.0,
        audio_sha256="abc",
    )

    items = list(adapter.transcribe_many([req]))

    assert len(items) == 1
    assert isinstance(items[0], SttFailure)
    assert items[0].category == "transcription_error"


def test_faster_whisper_adapter_uses_auto_language_detection(monkeypatch, tmp_path):
    monkeypatch.setitem(
        sys.modules,
        "faster_whisper",
        SimpleNamespace(WhisperModel=_WhisperModelCaptureLang),
    )

    adapter = FasterWhisperSttAdapter(_build_config())  # type: ignore[arg-type]
    req = SttRequest(
        call_id="c1",
        audio_path=tmp_path / "a.wav",
        audio_seconds=1.0,
        audio_sha256="abc",
        language="auto",
    )

    items = list(adapter.transcribe_many([req]))

    assert len(items) == 1
    assert isinstance(items[0], SttResult)
    assert _WhisperModelCaptureLang.last_language is None
