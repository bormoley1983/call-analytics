import json

from src.adapters.reporting_json import JsonReportingSource
from src.api.routes import reports as report_routes
from src.api.schemas import ManagersSortQuery, ReportFiltersQuery
from src.core.reporting_service import build_managers_report, build_overall_report
from src.domain.reporting import ReportFilters


def _write_analysis(base, call_id, **overrides):
    payload = {
        "manager_id": "sales_001",
        "manager_name": "Manager 1",
        "role": "sales",
        "spam_probability": 0.2,
        "effective_call": True,
        "intent": "консультація",
        "outcome": "продаж",
        "summary": "summary",
        "key_questions": ["Where is my order?"],
        "objections": [],
        "call_meta": {
            "direction": "incoming",
            "date": "20241112",
            "audio_seconds": 120.5,
        },
    }
    payload.update(overrides)
    path = base / f"{call_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_build_overall_report_from_json_source(tmp_path):
    _write_analysis(tmp_path, "call-1")
    _write_analysis(
        tmp_path,
        "call-2",
        manager_id="sales_002",
        manager_name="Manager 2",
        spam_probability=0.95,
        effective_call=False,
        intent="скарга",
        outcome="невідомо",
        key_questions=["Need refund", "Where is my order?"],
        call_meta={"direction": "outgoing", "date": "20241113", "audio_seconds": 30},
    )

    report = build_overall_report(
        JsonReportingSource(tmp_path),
        ReportFilters(),
        spam_threshold=0.7,
    )

    assert report["data_source"] == "json"
    assert report["total_calls"] == 2
    assert report["analyzed_calls"] == 2
    assert report["unique_managers"] == 2
    assert report["spam_calls"] == 1
    assert report["effective_calls"] == 1
    assert report["top_intents"][0] == ("консультація", 1)
    assert report["top_questions"][0] == ("where is my order?", 2)


def test_overall_route_uses_storage_backed_filters(monkeypatch, tmp_path):
    _write_analysis(tmp_path, "call-1", manager_id="sales_001", effective_call=True)
    _write_analysis(
        tmp_path,
        "call-2",
        manager_id="sales_002",
        effective_call=False,
        call_meta={"direction": "incoming", "date": "20241115", "audio_seconds": 10},
    )

    monkeypatch.setenv("SPAM_PROBABILITY_THRESHOLD", "0.7")
    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: JsonReportingSource(tmp_path))

    response = report_routes.overall_report(ReportFiltersQuery(manager_id="sales_001", effective_only=True))

    assert response["total_calls"] == 1
    assert response["effective_calls"] == 1
    assert response["filters"]["manager_id"] == "sales_001"


def test_build_managers_report_from_json_source(tmp_path):
    _write_analysis(tmp_path, "call-1", manager_id="sales_001", manager_name="Manager 1")
    _write_analysis(
        tmp_path,
        "call-2",
        manager_id="sales_001",
        manager_name="Manager 1",
        effective_call=False,
        spam_probability=0.95,
        call_meta={"direction": "outgoing", "date": "20241113", "audio_seconds": 30},
    )
    _write_analysis(
        tmp_path,
        "call-3",
        manager_id="sales_002",
        manager_name="Manager 2",
        role="sales",
        intent="скарга",
        outcome="невідомо",
    )

    report = build_managers_report(
        JsonReportingSource(tmp_path),
        ReportFilters(),
        spam_threshold=0.7,
    )

    assert report["data_source"] == "json"
    assert report["total_managers"] == 2
    assert report["role_summary"]["sales"]["total_calls"] == 3
    assert len(report["all_managers"]) == 2
    assert report["all_managers"][0]["manager_id"] == "sales_001"
    assert report["all_managers"][0]["total_calls"] == 2
    assert report["all_managers"][0]["spam_calls"] == 1


def test_manager_routes_use_storage_backed_report(monkeypatch, tmp_path):
    _write_analysis(tmp_path, "call-1", manager_id="sales_001", manager_name="Manager 1")
    _write_analysis(
        tmp_path,
        "call-2",
        manager_id="sales_002",
        manager_name="Manager 2",
        effective_call=False,
    )

    monkeypatch.setenv("SPAM_PROBABILITY_THRESHOLD", "0.7")
    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: JsonReportingSource(tmp_path))

    managers_response = report_routes.managers_report(ReportFiltersQuery(), ManagersSortQuery())
    manager_response = report_routes.manager_report("sales_001", ReportFiltersQuery())

    assert managers_response["total_managers"] == 2
    assert manager_response["manager_id"] == "sales_001"
    assert manager_response["total_calls"] == 1


def test_managers_report_supports_sorting(monkeypatch, tmp_path):
    _write_analysis(tmp_path, "call-1", manager_id="sales_001", manager_name="Manager 1")
    _write_analysis(tmp_path, "call-2", manager_id="sales_002", manager_name="Manager 2")
    _write_analysis(tmp_path, "call-3", manager_id="sales_002", manager_name="Manager 2")

    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: JsonReportingSource(tmp_path))

    response = report_routes.managers_report(
        ReportFiltersQuery(),
        ManagersSortQuery(sort_by="manager_name", order="asc"),
    )

    assert response["all_managers"][0]["manager_name"] == "Manager 1"
