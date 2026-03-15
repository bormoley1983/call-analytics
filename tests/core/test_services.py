import pytest

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
