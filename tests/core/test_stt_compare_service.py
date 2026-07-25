from adapters.stt_runs_json import JsonSttRunStore
from core.stt_compare_service import SttCompareService
from domain.stt_runs import SttRunManifest, SttRunResultRecord


def _mk_run(run_id: str, provider: str, model: str) -> SttRunManifest:
    return SttRunManifest(
        run_id=run_id,
        name=provider,
        purpose="benchmark",
        provider=provider,
        model_id=model,
        model_revision="unknown",
        config_hash=f"cfg-{provider}",
    )


def test_stt_compare_service_compares_common_calls(tmp_path):
    store = JsonSttRunStore(tmp_path)
    store.ensure_ready()

    baseline = _mk_run("11111111-1111-1111-1111-111111111111", "faster_whisper", "large")
    candidate = _mk_run("22222222-2222-2222-2222-222222222222", "canary", "canary-1b")
    store.create_run(baseline)
    store.create_run(candidate)

    store.upsert_result(
        SttRunResultRecord(
            run_id=baseline.run_id,
            call_id="call-1",
            audio_sha256="sha-1",
            audio_seconds=12.0,
            status="ok",
            canonical_payload={"text": "добрий день це тест дзвінка"},
            rtf=0.5,
        )
    )
    store.upsert_result(
        SttRunResultRecord(
            run_id=baseline.run_id,
            call_id="call-2",
            audio_sha256="sha-2",
            audio_seconds=8.0,
            status="ok",
            canonical_payload={"text": "потрібна консультація по товару"},
            rtf=0.45,
        )
    )

    store.upsert_result(
        SttRunResultRecord(
            run_id=candidate.run_id,
            call_id="call-1",
            audio_sha256="sha-1",
            audio_seconds=12.0,
            status="ok",
            canonical_payload={"text": "добрий день це тест"},
            rtf=0.25,
        )
    )
    store.upsert_result(
        SttRunResultRecord(
            run_id=candidate.run_id,
            call_id="call-3",
            audio_sha256="sha-3",
            audio_seconds=6.0,
            status="failed",
            canonical_payload={"text": ""},
            rtf=0.0,
            error_category="decode_error",
        )
    )

    service = SttCompareService(store)
    summary = service.compare_runs(baseline.run_id, candidate.run_id, top_n=5)

    assert summary["coverage"]["baseline_results"] == 2
    assert summary["coverage"]["candidate_results"] == 2
    assert summary["coverage"]["common_results"] == 1
    assert summary["coverage"]["baseline_only"] == 1
    assert summary["coverage"]["candidate_only"] == 1

    assert summary["quality"]["mean_text_similarity"] >= 0.0
    assert len(summary["worst_calls"]) == 1
    assert summary["worst_calls"][0]["call_id"] == "call-1"
