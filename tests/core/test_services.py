import pytest
from pathlib import Path
from types import SimpleNamespace

from api import runner
from api.schemas import ProcessRequest, SyncRequest
from core import pipeline, reports, rules, transcription
from domain import config


def test_pipeline_class_exists():
    assert hasattr(pipeline, "Pipeline")

def test_aggregate_report_exists():
    assert hasattr(reports, "aggregate_report")

def test_sha12_exists():
    assert hasattr(rules, "sha12")

def test_transcribe_exists():
    assert hasattr(transcription, "transcribe")

def test_appconfig_exists():
    assert hasattr(config, "AppConfig")


def test_configure_process_env_preserves_zero_limit(monkeypatch):
    monkeypatch.setenv("PROCESS_LIMIT", "30")

    runner._configure_process_env(
        ProcessRequest(days=None, limit=0, force_reanalyze=False, force_retranscribe=False)
    )

    assert runner.os.environ["PROCESS_LIMIT"] == "0"


def test_configure_process_env_sets_snapshot_toggle(monkeypatch):
    monkeypatch.delenv("GENERATE_REPORT_SNAPSHOTS", raising=False)

    runner._configure_process_env(
        ProcessRequest(
            days=None,
            limit=1,
            force_reanalyze=False,
            force_retranscribe=False,
            generate_report_snapshots=True,
        )
    )

    assert runner.os.environ["GENERATE_REPORT_SNAPSHOTS"] == "1"


def test_configure_process_env_unsets_snapshot_toggle_when_not_specified(monkeypatch):
    monkeypatch.setenv("GENERATE_REPORT_SNAPSHOTS", "1")

    runner._configure_process_env(
        ProcessRequest(
            days=None,
            limit=1,
            force_reanalyze=False,
            force_retranscribe=False,
            generate_report_snapshots=None,
        )
    )

    assert "GENERATE_REPORT_SNAPSHOTS" not in runner.os.environ


def _minimal_pipeline_config(generate_report_snapshots: bool):
    return SimpleNamespace(
        whisper_model="large-v3-turbo",
        whisper_device="cpu",
        whisper_compute_type="float32",
        ollama_model="test-model",
        analysis_workers=1,
        process_limit=1,
        generate_report_snapshots=generate_report_snapshots,
    )


def test_pipeline_run_syncs_even_when_snapshot_reports_disabled(monkeypatch):
    events = []
    cfg = _minimal_pipeline_config(generate_report_snapshots=False)
    pl = pipeline.Pipeline(config=cfg, storage=object(), audio=object(), llm=object(), pbx=object())

    monkeypatch.setattr(pipeline, "discover_and_filter_files", lambda config, storage: [Path("call.wav")])
    monkeypatch.setattr(pipeline, "categorize_files", lambda all_files, config, storage: ([], []))
    monkeypatch.setattr(pl, "run_transcription_phase", lambda files: [])
    monkeypatch.setattr(pl, "run_translation_phase", lambda files_metadata: files_metadata)
    monkeypatch.setattr(pl, "run_analysis_phase", lambda files_metadata: [{"status": "processed"}])
    monkeypatch.setattr(pl, "sync_to_postgres", lambda per_call: events.append("sync"))
    monkeypatch.setattr(pl, "generate_reports", lambda per_call: events.append("reports"))

    pl.run()

    assert events == ["sync"]


def test_pipeline_run_generates_snapshots_only_after_sync(monkeypatch):
    events = []
    cfg = _minimal_pipeline_config(generate_report_snapshots=True)
    pl = pipeline.Pipeline(config=cfg, storage=object(), audio=object(), llm=object(), pbx=object())

    monkeypatch.setattr(pipeline, "discover_and_filter_files", lambda config, storage: [Path("call.wav")])
    monkeypatch.setattr(pipeline, "categorize_files", lambda all_files, config, storage: ([], []))
    monkeypatch.setattr(pl, "run_transcription_phase", lambda files: [])
    monkeypatch.setattr(pl, "run_translation_phase", lambda files_metadata: files_metadata)
    monkeypatch.setattr(pl, "run_analysis_phase", lambda files_metadata: [{"status": "processed"}])
    monkeypatch.setattr(pl, "sync_to_postgres", lambda per_call: events.append("sync"))
    monkeypatch.setattr(pl, "generate_reports", lambda per_call: events.append("reports"))

    pl.run()

    assert events == ["sync", "reports"]


def test_run_keyword_refresh_once_skips_without_postgres(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    assert runner._run_keyword_refresh_once() is None


def test_run_keyword_refresh_once_skips_when_disabled(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.setenv("AUTO_REFRESH_KEYWORDS", "0")

    assert runner._run_keyword_refresh_once() is None


def test_run_process_once_includes_keywords_refresh(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.delenv("AUTO_REFRESH_KEYWORDS", raising=False)

    class FakeStorage:
        def ensure_ready(self):
            return None

        def close(self):
            return None

    class FakePipeline:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self):
            return None

    monkeypatch.setattr(runner, "load_app_config", lambda: SimpleNamespace(out=Path("."), norm=Path("."), trans=Path("."), analysis=Path(".")))
    monkeypatch.setattr(runner, "PostgresStorage", lambda dsn: FakeStorage())
    monkeypatch.setattr(runner, "Pipeline", FakePipeline)
    monkeypatch.setattr(runner, "OllamaLlm", lambda config: object())
    monkeypatch.setattr(runner, "FfmpegAudio", lambda: object())
    monkeypatch.setattr(runner, "AsteriskPbx", lambda: object())
    monkeypatch.setattr(runner, "_run_keyword_refresh_once", lambda prune_missing=False: {"sync": {"synced": 2}})
    monkeypatch.setattr(runner, "_run_keyword_ai_analysis_once", lambda trigger: {"analysis_history": {"analysis_id": "a1"}})

    result = runner._run_process_once(
        ProcessRequest(days=None, limit=1, force_reanalyze=False, force_retranscribe=False)
    )

    assert result["ok"] is True
    assert result["keywords_refresh"]["sync"]["synced"] == 2
    assert result["keyword_ai_analysis"]["analysis_history"]["analysis_id"] == "a1"


def test_run_process_once_keeps_success_when_keywords_refresh_fails(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.delenv("AUTO_REFRESH_KEYWORDS", raising=False)

    class FakeStorage:
        def ensure_ready(self):
            return None

        def close(self):
            return None

    class FakePipeline:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self):
            return None

    monkeypatch.setattr(runner, "load_app_config", lambda: SimpleNamespace(out=Path("."), norm=Path("."), trans=Path("."), analysis=Path(".")))
    monkeypatch.setattr(runner, "PostgresStorage", lambda dsn: FakeStorage())
    monkeypatch.setattr(runner, "Pipeline", FakePipeline)
    monkeypatch.setattr(runner, "OllamaLlm", lambda config: object())
    monkeypatch.setattr(runner, "FfmpegAudio", lambda: object())
    monkeypatch.setattr(runner, "AsteriskPbx", lambda: object())
    monkeypatch.setattr(runner, "_run_keyword_refresh_once", lambda prune_missing=False: (_ for _ in ()).throw(RuntimeError("refresh failed")))
    monkeypatch.setattr(runner, "_run_keyword_ai_analysis_once", lambda trigger: None)

    result = runner._run_process_once(
        ProcessRequest(days=None, limit=1, force_reanalyze=False, force_retranscribe=False)
    )

    assert result["ok"] is True
    assert result["keywords_refresh_error"] == "refresh failed"


def test_auto_keyword_ai_analysis_enabled_uses_analysis_env(monkeypatch):
    monkeypatch.setenv("AUTO_RUN_AI_KEYWORD_ANALYSIS", "0")

    assert runner._auto_keyword_ai_analysis_enabled() is False


def test_run_sync_does_not_trigger_keyword_ai_analysis(monkeypatch):
    updates = []

    monkeypatch.setattr(runner.job_store, "update_job", lambda job_id, **kwargs: updates.append(kwargs))
    monkeypatch.setattr(runner, "_run_sync_once", lambda req: {"downloaded": 2, "downloaded_days": ["2026/03/20"]})
    monkeypatch.setattr(
        runner,
        "_run_keyword_ai_analysis_once",
        lambda trigger: (_ for _ in ()).throw(AssertionError("sync should not trigger AI analysis")),
    )

    runner.run_sync("job-1", SyncRequest(days="2026/03/20"))

    assert updates[-1]["status"] == runner.JobStatus.done
    assert updates[-1]["result"]["downloaded"] == 2
    assert "keyword_ai_analysis" not in updates[-1]["result"]


def test_run_sync_and_process_uses_requested_days_for_sync(monkeypatch):
    updates = []
    captured = {}

    monkeypatch.setattr(runner.job_store, "update_job", lambda job_id, **kwargs: updates.append(kwargs))

    def fake_sync(req):
        captured["days"] = req.days
        return {"downloaded": 1, "downloaded_days": ["2026/03/19"]}

    monkeypatch.setattr(runner, "_run_sync_once", fake_sync)
    monkeypatch.setattr(runner, "_run_process_once", lambda req: {"ok": True})

    runner.run_sync_and_process(
        "job-2",
        ProcessRequest(days="2026/03/19", limit=1, force_reanalyze=False, force_retranscribe=False),
    )

    assert captured["days"] == "2026/03/19"
    assert updates[-1]["status"] == runner.JobStatus.done
    assert updates[-1]["result"]["process"]["downloaded"] == 1
