import pytest
from pathlib import Path
from types import SimpleNamespace

from src.api import runner
from src.api.schemas import ProcessRequest
from src.core import pipeline, reports, rules, transcription
from src.domain import config


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
