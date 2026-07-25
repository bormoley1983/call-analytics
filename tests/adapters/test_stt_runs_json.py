from adapters.stt_runs_json import JsonSttRunStore
from domain.stt_runs import SttRunManifest, SttRunResultRecord


def test_json_stt_run_store_creates_manifest_and_result(tmp_path):
    store = JsonSttRunStore(tmp_path)
    store.ensure_ready()

    run = SttRunManifest(
        run_id="11111111-1111-1111-1111-111111111111",
        name="baseline",
        purpose="benchmark",
        provider="faster_whisper",
        model_id="large-v3-turbo",
        model_revision="unknown",
        config_hash="abc123",
    )
    store.create_run(run)

    result = SttRunResultRecord(
        run_id=run.run_id,
        call_id="call-1",
        audio_sha256="sha",
        audio_seconds=12.0,
        status="ok",
        canonical_payload={"text": "hello"},
    )
    store.upsert_result(result)

    loaded_run = store.load_run(run.run_id)
    loaded_result = store.load_result(run.run_id, "call-1")

    assert loaded_run.run_id == run.run_id
    assert loaded_result.call_id == "call-1"
    assert loaded_result.canonical_payload["text"] == "hello"


def test_json_stt_run_store_iter_results(tmp_path):
    store = JsonSttRunStore(tmp_path)
    store.ensure_ready()

    run = SttRunManifest(
        run_id="22222222-2222-2222-2222-222222222222",
        name="candidate",
        purpose="benchmark",
        provider="canary",
        model_id="canary-1b",
        model_revision="unknown",
        config_hash="cfg-2",
    )
    store.create_run(run)

    store.upsert_result(
        SttRunResultRecord(
            run_id=run.run_id,
            call_id="call-a",
            audio_sha256="a",
            audio_seconds=3.0,
            status="ok",
            canonical_payload={"text": "one"},
        )
    )
    store.upsert_result(
        SttRunResultRecord(
            run_id=run.run_id,
            call_id="call-b",
            audio_sha256="b",
            audio_seconds=4.0,
            status="ok",
            canonical_payload={"text": "two"},
        )
    )

    items = list(store.iter_results(run.run_id))
    assert [i.call_id for i in items] == ["call-a", "call-b"]
