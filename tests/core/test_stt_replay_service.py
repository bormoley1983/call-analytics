from pathlib import Path
from types import SimpleNamespace

from adapters.storage_json import JsonStorage
from adapters.stt_runs_json import JsonSttRunStore
from core.stt_replay_service import SttReplayService
from domain.stt import SttIdentity, SttResult, SttSegment


class _FakeAudio:
    def normalize(self, src: Path, dst: Path) -> None:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())

    def duration_seconds(self, path: Path) -> float:
        return 2.0


class _FakeStt:
    @property
    def identity(self):
        return SttIdentity(
            provider="fake",
            model_id="m1",
            model_revision="r1",
            config_hash="h1",
        )

    def transcribe_many(self, requests):
        req = requests[0]
        yield SttResult(
            call_id=req.call_id,
            language="uk",
            duration=req.audio_seconds,
            segments=[SttSegment(start=0.0, end=2.0, text="hello")],
            raw_text="hello",
            warnings=[],
            timings={},
        )

    def close(self):
        return None


def test_replay_persists_run_results_and_promotes(tmp_path):
    raw = tmp_path / "calls_raw" / "2026" / "07" / "25"
    raw.mkdir(parents=True, exist_ok=True)
    src = raw / "rec-1.wav"
    src.write_bytes(b"x" * 30000)

    out = tmp_path / "out"
    norm = out / "normalized"
    trans = out / "transcripts"
    analysis = out / "analysis"

    storage = JsonStorage(out, norm, trans, analysis)
    storage.ensure_ready()
    run_store = JsonSttRunStore(out)
    run_store.ensure_ready()

    config = SimpleNamespace(
        stt_engine="faster-whisper",
        stt_language="auto",
        whisper_model="large",
        canary_model_id="canary",
        canary_device="cpu",
        min_bytes=20000,
        min_seconds=1.0,
        norm=norm,
    )

    service = SttReplayService(
        config=config,
        audio=_FakeAudio(),
        stt=_FakeStt(),
        run_store=run_store,
        active_storage=storage,
    )
    result = service.replay(
        target_files=[src],
        run_name="test-run",
        purpose="replay",
        promote_to_active=True,
    )

    assert result["counts"]["completed"] == 1

    run_id = result["run_id"]
    run = run_store.load_run(run_id)
    assert run.name == "test-run"

    call_id = next(iter(trans.glob("*.json"))).stem
    transcript = storage.load_transcript(call_id)
    assert transcript["stt_run_id"] == run_id
    assert transcript["source_text_sha256"]
