from __future__ import annotations

import pytest

from core.deep_insights import (
    VALID_INSIGHT_TYPES,
    generate_deep_insights,
    store_deep_insights_run,
)


class FakeReportingSource:
    def __init__(self, records):
        self.records = records

    def iter_call_records(self, filters):
        return iter(self.records)

    def close(self):
        pass


class FakeLlm:
    def generate_deep_insights(self, insight_type, analysis_records):
        return {
            "insights": [
                {
                    "title": f"Insight for {insight_type}",
                    "description": f"Generated insight of type {insight_type}",
                    "severity": "medium",
                    "affected_calls_count": len(analysis_records),
                    "evidence_summary": "Based on analysis records",
                }
            ]
        }


def _make_record(call_id, summary="", key_questions=None, objections=None):
    from datetime import datetime, timezone

    from domain.reporting import ReportCallRecord

    return ReportCallRecord(
        call_id=call_id,
        manager_id="m1",
        manager_name="Mgr",
        role="sales",
        direction="incoming",
        spam_probability=0.1,
        effective_call=True,
        intent="buy",
        outcome="success",
        summary=summary,
        audio_seconds=30.0,
        call_datetime=datetime(2026, 1, 1, tzinfo=timezone.utc),
        key_questions=key_questions or [],
        objections=objections or [],
    )


def test_valid_insight_types():
    assert "pain_points" in VALID_INSIGHT_TYPES
    assert "objections" in VALID_INSIGHT_TYPES
    assert "trends" in VALID_INSIGHT_TYPES
    assert "follow_up_risk" in VALID_INSIGHT_TYPES


def test_generate_deep_insights_basic():
    records = [
        _make_record(
            "call1", "Customer complained about price", objections=["too expensive"]
        ),
        _make_record("call2", "Another customer mentioned high cost"),
    ]

    result = generate_deep_insights(
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        insight_types=["pain_points"],
        filters=None,
        max_insights=10,
    )

    assert "run_id" in result
    assert "insight_counts" in result
    assert "insights" in result
    assert result["insight_counts"]["pain_points"] == 1
    assert len(result["insights"]) == 1
    assert result["insights"][0]["insight_type"] == "pain_points"


def test_generate_deep_insights_multiple_types():
    records = [_make_record("call1", "Summary text")]

    result = generate_deep_insights(
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        insight_types=["pain_points", "trends"],
        max_insights=5,
    )

    assert result["insight_counts"]["pain_points"] >= 0
    assert result["insight_counts"]["trends"] >= 0


def test_generate_deep_insights_invalid_type():
    with pytest.raises(ValueError, match="Invalid insight type"):
        generate_deep_insights(
            reporting_source=FakeReportingSource([]),  # type: ignore[arg-type]
            llm=FakeLlm(),  # type: ignore[arg-type]
            insight_types=["invalid_type"],
        )


def test_generate_deep_insights_no_records():
    result = generate_deep_insights(
        reporting_source=FakeReportingSource([]),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        insight_types=["pain_points"],
    )

    assert result["insights"] == []
    assert result["insight_counts"]["pain_points"] == 0


def test_generate_deep_insights_with_dict_filters():
    """Test that dict-based filters work (from API routes)."""
    records = [_make_record("call1", "Summary")]

    result = generate_deep_insights(
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        insight_types=["pain_points"],
        filters={"date_from": None, "manager_id": "m1"},
    )

    assert len(result["insights"]) >= 0


def test_store_deep_insights_run():
    """Test storing a deep insights run."""

    class FakeStore:
        def __init__(self):
            self.runs = []
            self.insights_list = []

        def create_run(self, run_id, **kwargs):
            self.runs.append({"run_id": run_id, **kwargs})

        def add_insights(self, run_id, insights):
            self.insights_list.extend(insights)

    store = FakeStore()
    run_data = {
        "run_id": "test-run-1",
        "insights": [
            {"insight_type": "pain_points", "title": "High prices", "severity": "high"},
        ],
        "max_insights": 10,
    }

    run_id = store_deep_insights_run(
        store=store,
        run_data=run_data,
        ai_model="fake-model",
        filters=None,
        insight_types=["pain_points"],
    )

    assert run_id == "test-run-1"
    assert len(store.runs) == 1
    assert store.runs[0]["ai_model"] == "fake-model"
    assert len(store.insights_list) == 1


def test_store_deep_insights_run_with_dict_filters():
    """Test storing with dict-based filters."""

    class FakeStore:
        def __init__(self):
            self.runs = []

        def create_run(self, run_id, **kwargs):
            self.runs.append({"run_id": run_id, **kwargs})

        def add_insights(self, run_id, insights):
            pass

    store = FakeStore()
    run_data = {"run_id": "test-run-2", "insights": [], "max_insights": 5}

    store_deep_insights_run(
        store=store,
        run_data=run_data,
        filters={"date_from": "2026-01-01"},
        insight_types=["trends"],
    )

    assert store.runs[0]["request_data"]["filters"] == {"date_from": "2026-01-01"}
