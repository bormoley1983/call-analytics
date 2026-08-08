from pathlib import Path
from types import SimpleNamespace

from adapters.stt_canary import CanarySttAdapter
from domain.stt import SttFailure, SttRequest


def _build_config():
    return SimpleNamespace(
        stt_engine="canary",
        canary_model_id="nvidia/canary-1b-v2",
        canary_model_revision="unknown",
        canary_device="cpu",
        canary_compute_type="float16",
        canary_batch_size=1,
        canary_beam_size=1,
    )


def test_canary_adapter_returns_typed_init_failure_when_runtime_missing(tmp_path):
    adapter = CanarySttAdapter(_build_config())  # type: ignore[arg-type]
    req = SttRequest(
        call_id="c1",
        audio_path=tmp_path / "a.wav",
        audio_seconds=1.0,
        audio_sha256="abc",
    )

    items = list(adapter.transcribe_many([req]))

    assert len(items) == 1
    assert isinstance(items[0], SttFailure)
    assert items[0].category == "model_init_error"


def test_canary_kwargs_omit_source_target_when_auto_language():
    adapter = CanarySttAdapter(_build_config())  # type: ignore[arg-type]
    req = SttRequest(
        call_id="c1",
        audio_path=Path("/tmp/unused.wav"),
        audio_seconds=1.0,
        audio_sha256="abc",
        language="auto",
    )

    kwargs = adapter._transcribe_kwargs(req)

    assert "source_lang" not in kwargs
    assert "target_lang" not in kwargs
