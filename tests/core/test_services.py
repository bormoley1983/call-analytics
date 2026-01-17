import pytest

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