from types import SimpleNamespace

from core.stt_service import SttService
from domain.stt import SttIdentity, SttRequest, SttResult, SttSegment


class _FakeProcessor:
    def __init__(self):
        self._identity = SttIdentity(
            provider="fake",
            model_id="m1",
            model_revision="r1",
            config_hash="h1",
        )

    @property
    def identity(self):
        return self._identity

    def transcribe_many(self, requests):
        req = requests[0]
        yield SttResult(
            call_id=req.call_id,
            language="uk",
            duration=12.3,
            segments=[
                SttSegment(start=0.0, end=1.0, text="kse text"),
                SttSegment(start=1.0, end=2.0, text="   "),
            ],
            raw_text="kse text",
            timings={"infer": 0.3},
        )

    def close(self):
        return None


def test_stt_service_applies_brand_corrections_and_builds_transcript(tmp_path):
    config = SimpleNamespace(brand_corrections={"kse": "KSE"})
    service = SttService(_FakeProcessor(), config)

    request = SttRequest(
        call_id="c1",
        audio_path=tmp_path / "a.wav",
        audio_seconds=12.3,
        audio_sha256="abc",
        language="uk",
    )

    transcript = service.transcribe_one(request)

    assert transcript["language"] == "uk"
    assert transcript["duration"] == 12.3
    assert transcript["text"] == "KSE text"
    assert transcript["segments"] == [{"start": 0.0, "end": 1.0, "text": "KSE text"}]
    assert transcript["_stt"]["provider"] == "fake"
    assert transcript["_stt"]["model_id"] == "m1"
