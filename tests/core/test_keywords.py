import copy
import json

import yaml
import pytest
from fastapi import HTTPException

from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from api.routes import keywords as keyword_routes
from api.routes import reports as report_routes
from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from api.schemas import (
    KeywordCallsSortQuery,
    KeywordManagersSortQuery,
    KeywordsSortQuery,
    KeywordSyncRequest,
    KeywordUpsertRequest,
    PaginationQuery,
    ReportFiltersQuery,
)
from core.keywords_materialize import materialize_call_keywords
from core.keywords_service import build_keywords_report, list_keywords
from core.keywords_sync import sync_keywords_to_postgres
from domain.keywords import KeywordDefinition
from domain.reporting import ReportFilters


def _write_analysis(base, call_id, **overrides):
    payload = {
        "manager_id": "sales_001",
        "manager_name": "Manager 1",
        "role": "sales",
        "spam_probability": 0.2,
        "effective_call": True,
        "intent": "консультація",
        "outcome": "продаж",
        "summary": "Client asked about delivery and refund options.",
        "key_questions": ["Where is my order?"],
        "objections": ["Delivery is too expensive"],
        "call_meta": {
            "direction": "incoming",
            "date": "20241112",
            "audio_seconds": 120.5,
        },
    }
    payload.update(overrides)
    path = base / f"{call_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_keywords(path):
    payload = [
        {
            "keyword_id": "delivery",
            "label": "Delivery",
            "category": "logistics",
            "terms": ["delivery", "order"],
            "match_fields": ["summary", "key_questions", "objections"],
            "is_active": True,
        },
        {
            "keyword_id": "refund",
            "label": "Refund",
            "category": "payments",
            "terms": ["refund"],
            "match_fields": ["summary"],
            "is_active": True,
        },
    ]
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _write_invalid_keywords(path):
    path.write_text("keywords: [", encoding="utf-8")


def test_list_keywords_from_yaml_source(tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)

    response = list_keywords(YamlKeywordSource(keywords_path))

    assert response["data_source"] == "yaml"
    assert response["total_keywords"] == 2
    assert response["keywords"][0]["keyword_id"] == "delivery"


def test_build_keywords_report_from_yaml_sources(tmp_path):
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)

    _write_analysis(analysis_dir, "call-1")
    _write_analysis(
        analysis_dir,
        "call-2",
        summary="Refund requested because delivery was delayed.",
        key_questions=["Refund status?"],
        objections=[],
        manager_id="sales_002",
    )

    response = build_keywords_report(
        JsonReportingSource(analysis_dir),
        YamlKeywordSource(keywords_path),
        ReportFilters(),
        spam_threshold=0.7,
    )

    assert response["total_keywords"] == 2
    assert response["keywords_with_matches"] == 2
    by_id = {item["keyword_id"]: item for item in response["keywords"]}
    assert by_id["delivery"]["matched_calls"] == 2
    assert by_id["refund"]["matched_calls"] == 2


def test_keyword_routes_use_storage_backed_sources(monkeypatch, tmp_path):
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)
    _write_analysis(analysis_dir, "call-1")

    monkeypatch.setenv("SPAM_PROBABILITY_THRESHOLD", "0.7")
    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: JsonReportingSource(analysis_dir))
    monkeypatch.setattr(report_routes, "_get_keyword_source", lambda: YamlKeywordSource(keywords_path))
    monkeypatch.setattr(keyword_routes, "_get_keyword_source", lambda: YamlKeywordSource(keywords_path))

    catalog = keyword_routes.keywords_catalog()
    report = report_routes.keywords_report(ReportFiltersQuery(), KeywordsSortQuery())
    detail = report_routes.keyword_detail_report("delivery", ReportFiltersQuery())

    assert catalog["total_keywords"] == 2
    assert report["keywords_with_matches"] >= 1
    assert detail["keyword_id"] == "delivery"


def test_keywords_catalog_fails_loudly_on_invalid_yaml(monkeypatch, tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_invalid_keywords(keywords_path)

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(keyword_routes, "KEYWORDS_CONFIG", keywords_path)

    with pytest.raises(HTTPException) as exc:
        keyword_routes.keywords_catalog()

    assert exc.value.status_code == 500
    assert "Invalid keyword YAML" in exc.value.detail


def test_keywords_report_fails_loudly_on_invalid_yaml(monkeypatch, tmp_path):
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()
    _write_analysis(analysis_dir, "call-1")
    keywords_path = tmp_path / "keywords.yaml"
    _write_invalid_keywords(keywords_path)

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(report_routes, "KEYWORDS_CONFIG", keywords_path)
    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: JsonReportingSource(analysis_dir))

    with pytest.raises(HTTPException) as exc:
        report_routes.keywords_report(ReportFiltersQuery(), KeywordsSortQuery())

    assert exc.value.status_code == 500
    assert "Invalid keyword YAML" in exc.value.detail


class FakeWritableKeywordSource:
    def __init__(self, initial=None):
        self.items = {item.keyword_id: item for item in (initial or [])}
        self.materialized_state = False

    def get_keyword(self, keyword_id):
        return self.items.get(keyword_id)

    def upsert_keyword(self, keyword):
        self.items[keyword.keyword_id] = keyword
        return keyword

    def delete_keyword(self, keyword_id):
        return self.items.pop(keyword_id, None) is not None

    def list_keywords(self):
        return list(self.items.values())

    def close(self):
        return None

    def replace_call_keyword_matches(self, call_id, rows):
        self.last_replaced = (call_id, rows)

    def is_materialized(self):
        return self.materialized_state

    def mark_materialization_completed(self, processed_calls, matched_calls, stored_rows):
        self.materialized_state = True
        self.last_materialization = {
            "processed_calls": processed_calls,
            "matched_calls": matched_calls,
            "stored_rows": stored_rows,
        }


class FakeReportingSource:
    def __init__(self, records):
        self.records = records

    def iter_call_records(self, filters):
        return iter(self.records)

    def close(self):
        return None


def test_keyword_management_routes_use_writable_source(monkeypatch):
    source = FakeWritableKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary"],
                is_active=True,
            )
        ]
    )
    monkeypatch.setattr(keyword_routes, "_get_writable_keyword_source", lambda: source)

    created = keyword_routes.create_keyword(
        KeywordUpsertRequest(
            keyword_id="refund",
            label="Refund",
            category="payments",
            terms=["refund"],
            match_fields=["summary"],
            is_active=True,
        )
    )
    updated = keyword_routes.update_keyword(
        "refund",
        KeywordUpsertRequest(
            keyword_id="refund",
            label="Refund Updated",
            category="payments",
            terms=["refund", "return"],
            match_fields=["summary", "key_questions"],
            is_active=False,
        ),
    )
    keyword_routes.delete_keyword("refund")

    assert created["keyword_id"] == "refund"
    assert updated["label"] == "Refund Updated"
    assert source.get_keyword("refund") is None


def test_keyword_management_routes_enforce_read_only_without_postgres(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    with pytest.raises(HTTPException) as exc:
        keyword_routes.create_keyword(
            KeywordUpsertRequest(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary"],
                is_active=True,
            )
        )

    assert exc.value.status_code == 405
    assert "read-only" in exc.value.detail


def test_sync_keywords_to_postgres_supports_pruning(tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)
    postgres_source = FakeWritableKeywordSource(
        [
            KeywordDefinition(
                keyword_id="legacy",
                label="Legacy",
                category="general",
                terms=["legacy"],
                match_fields=["summary"],
                is_active=True,
            )
        ]
    )

    response = sync_keywords_to_postgres(
        yaml_source=YamlKeywordSource(keywords_path),
        postgres_source=postgres_source,
        prune_missing=True,
    )

    assert response["synced"] == 2
    assert response["deleted"] == 1
    assert response["deleted_keyword_ids"] == ["legacy"]
    assert sorted(postgres_source.items) == ["delivery", "refund"]


def test_sync_keywords_does_not_mutate_analysis_records(tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)

    analyses = {
        "call-1": {
            "summary": "Delivery issue, customer asks for refund",
            "key_questions": ["Where is delivery?"],
            "objections": ["Too slow"],
            "call_meta": {"date": "20241112"},
        }
    }
    analyses_before = copy.deepcopy(analyses)

    class FakeWritableKeywordSourceWithAnalyses(FakeWritableKeywordSource):
        def __init__(self, analyses_records):
            super().__init__()
            self.analyses = analyses_records

    postgres_source = FakeWritableKeywordSourceWithAnalyses(analyses)

    sync_keywords_to_postgres(
        yaml_source=YamlKeywordSource(keywords_path),
        postgres_source=postgres_source,
        prune_missing=False,
    )

    assert analyses == analyses_before


def test_keyword_sync_route_uses_yaml_and_writable_sources(monkeypatch, tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)
    postgres_source = FakeWritableKeywordSource()

    monkeypatch.setattr(keyword_routes, "_get_yaml_keyword_source", lambda strict=False: YamlKeywordSource(keywords_path, strict=strict))
    monkeypatch.setattr(keyword_routes, "_get_writable_keyword_source", lambda: postgres_source)
    monkeypatch.setattr(keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result)

    response = keyword_routes.sync_keywords(KeywordSyncRequest(prune_missing=False))

    assert response["synced"] == 2
    assert response["deleted"] == 0
    assert sorted(postgres_source.items) == ["delivery", "refund"]


def test_keyword_refresh_route_runs_sync_and_materialize(monkeypatch, tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    _write_keywords(keywords_path)
    postgres_source = FakeWritableKeywordSource()
    reporting_source = FakeReportingSource(
        [
            type(
                "Record",
                (),
                {
                    "call_id": "call-1",
                    "summary": "delivery and refund discussion",
                    "key_questions": ["Where is delivery?"],
                    "objections": [],
                    "manager_id": "sales_001",
                    "spam_probability": 0.1,
                    "effective_call": True,
                    "intent": "консультація",
                    "outcome": "продаж",
                },
            )()
        ]
    )

    monkeypatch.setattr(keyword_routes, "_get_yaml_keyword_source", lambda strict=False: YamlKeywordSource(keywords_path, strict=strict))
    monkeypatch.setattr(keyword_routes, "_get_writable_keyword_source", lambda: postgres_source)
    monkeypatch.setattr(keyword_routes, "_get_postgres_reporting_source", lambda: reporting_source)
    monkeypatch.setattr(keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result)

    response = keyword_routes.refresh_keywords(KeywordSyncRequest(prune_missing=False))

    assert response["sync"]["synced"] == 2
    assert response["materialize"]["processed_calls"] == 1
    assert response["materialize"]["matched_calls"] == 1
    assert postgres_source.materialized_state is True


def test_keyword_sync_route_rejects_missing_yaml(monkeypatch, tmp_path):
    postgres_source = FakeWritableKeywordSource()

    monkeypatch.setattr(
        keyword_routes,
        "_get_yaml_keyword_source",
        lambda strict=False: YamlKeywordSource(tmp_path / "missing.yaml", strict=strict),
    )
    monkeypatch.setattr(keyword_routes, "_get_writable_keyword_source", lambda: postgres_source)
    monkeypatch.setattr(keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result)

    with pytest.raises(HTTPException) as exc:
        keyword_routes.sync_keywords(KeywordSyncRequest(prune_missing=True))

    assert exc.value.status_code == 400
    assert "Keyword config not found" in exc.value.detail


def test_materialize_call_keywords_persists_matches():
    records = [
        type(
            "Record",
            (),
            {
                "call_id": "call-1",
                "summary": "delivery and refund discussion",
                "key_questions": ["Where is delivery?"],
                "objections": [],
                "manager_id": "sales_001",
                "spam_probability": 0.1,
                "effective_call": True,
                "intent": "консультація",
                "outcome": "продаж",
            },
        )()
    ]
    keyword_source = FakeWritableKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary", "key_questions"],
                is_active=True,
            ),
            KeywordDefinition(
                keyword_id="refund",
                label="Refund",
                category="payments",
                terms=["refund"],
                match_fields=["summary"],
                is_active=True,
            ),
        ]
    )

    response = materialize_call_keywords(
        FakeReportingSource(records),
        keyword_source,
        keyword_source,
        state_store=keyword_source,
    )

    assert response["processed_calls"] == 1
    assert response["matched_calls"] == 1
    assert response["stored_rows"] == 2
    assert keyword_source.last_replaced[0] == "call-1"
    assert {row["keyword_id"] for row in keyword_source.last_replaced[1]} == {"delivery", "refund"}
    assert keyword_source.materialized_state is True
    assert keyword_source.last_materialization["processed_calls"] == 1


def test_materialize_call_keywords_does_not_mutate_analysis_records():
    analyses = {
        "call-1": {
            "summary": "delivery and refund discussion",
            "key_questions": ["Where is delivery?"],
            "objections": ["Delivery is expensive"],
            "manager_id": "sales_001",
            "spam_probability": 0.1,
            "effective_call": True,
            "intent": "консультація",
            "outcome": "продаж",
        }
    }
    analyses_before = copy.deepcopy(analyses)

    class FakeReportingSourceFromAnalyses:
        def __init__(self, analyses_records):
            self.analyses = analyses_records

        def iter_call_records(self, filters):
            for call_id, payload in self.analyses.items():
                yield type(
                    "Record",
                    (),
                    {
                        "call_id": call_id,
                        "summary": payload["summary"],
                        "key_questions": payload["key_questions"],
                        "objections": payload["objections"],
                        "manager_id": payload["manager_id"],
                        "spam_probability": payload["spam_probability"],
                        "effective_call": payload["effective_call"],
                        "intent": payload["intent"],
                        "outcome": payload["outcome"],
                    },
                )()

        def close(self):
            return None

    keyword_source = FakeWritableKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary", "key_questions", "objections"],
                is_active=True,
            )
        ]
    )

    materialize_call_keywords(
        FakeReportingSourceFromAnalyses(analyses),
        keyword_source,
        keyword_source,
        state_store=keyword_source,
    )

    assert analyses == analyses_before


def test_keyword_report_route_prefers_materialized_postgres(monkeypatch):
    class FakeMaterializedKeywordSource(report_routes.PostgresKeywordSource):
        def __init__(self):
            pass

        def is_materialized(self):
            return True

        def build_materialized_keywords_report(self, filters, spam_threshold, sort_by, order):
            return {
                "report_data_source": "postgres_materialized",
                "keyword_data_source": "postgres",
                "filters": filters.as_dict(),
                "sort_by": sort_by,
                "order": order,
                "total_keywords": 1,
                "keywords_with_matches": 1,
                "keywords": [
                    {
                        "keyword_id": "delivery",
                        "label": "Delivery",
                        "category": "logistics",
                        "terms": ["delivery"],
                        "match_fields": ["summary"],
                        "matched_calls": 5,
                        "total_matches": 7,
                        "matched_managers": 2,
                        "top_intents": [],
                        "top_outcomes": [],
                    }
                ],
            }

        def close(self):
            return None

    class FakePostgresReportingSource(report_routes.PostgresReportingSource):
        def __init__(self):
            pass

        def close(self):
            return None

    monkeypatch.setattr(report_routes, "_get_reporting_source", lambda: FakePostgresReportingSource())
    monkeypatch.setattr(report_routes, "_get_keyword_source", lambda: FakeMaterializedKeywordSource())

    response = report_routes.keywords_report(ReportFiltersQuery(), KeywordsSortQuery())

    assert response["report_data_source"] == "postgres_materialized"
    assert response["sort_by"] == "matched_calls"
    assert response["keywords"][0]["matched_calls"] == 5


def test_keyword_drilldown_routes_use_materialized_source(monkeypatch):
    class FakeMaterializedKeywordSource(PostgresKeywordSource):
        def __init__(self):
            pass

        def is_materialized(self):
            return True

        def get_keyword(self, keyword_id):
            return KeywordDefinition(
                keyword_id=keyword_id,
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary"],
                is_active=True,
            )

        def build_keyword_calls_report(self, keyword_id, filters, spam_threshold, limit, offset, sort_by, order):
            return {
                "keyword_id": keyword_id,
                "report_data_source": "postgres_materialized",
                "total_calls": 1,
                "limit": limit,
                "offset": offset,
                "sort_by": sort_by,
                "order": order,
                "calls": [{"call_id": "call-1", "match_count": 2}],
            }

        def build_keyword_trend_report(self, keyword_id, filters, spam_threshold):
            return {
                "keyword_id": keyword_id,
                "report_data_source": "postgres_materialized",
                "points": [{"call_date": "20241112", "matched_calls": 1, "total_matches": 2}],
            }

        def build_keyword_managers_report(self, keyword_id, filters, spam_threshold, sort_by, order):
            return {
                "keyword_id": keyword_id,
                "report_data_source": "postgres_materialized",
                "sort_by": sort_by,
                "order": order,
                "managers": [{"manager_id": "sales_001", "matched_calls": 1, "total_matches": 2}],
            }

        def close(self):
            return None

    monkeypatch.setattr(report_routes, "_get_materialized_keyword_source", lambda: FakeMaterializedKeywordSource())

    calls = report_routes.keyword_calls_report(
        "delivery",
        ReportFiltersQuery(),
        pagination=PaginationQuery(limit=25, offset=0),
        sorting=KeywordCallsSortQuery(sort_by="match_count", order="asc"),
    )
    trend = report_routes.keyword_trend_report("delivery", ReportFiltersQuery())
    managers = report_routes.keyword_managers_report(
        "delivery",
        ReportFiltersQuery(),
        sorting=KeywordManagersSortQuery(sort_by="manager_name", order="asc"),
    )

    assert calls["calls"][0]["call_id"] == "call-1"
    assert calls["limit"] == 25
    assert calls["sort_by"] == "match_count"
    assert trend["points"][0]["matched_calls"] == 1
    assert managers["managers"][0]["manager_id"] == "sales_001"
    assert managers["sort_by"] == "manager_name"


def test_keyword_drilldown_route_returns_404_for_unknown_keyword(monkeypatch):
    class FakeMaterializedKeywordSource(PostgresKeywordSource):
        def __init__(self):
            pass

        def is_materialized(self):
            return True

        def get_keyword(self, keyword_id):
            return None

        def close(self):
            return None

    monkeypatch.setattr(report_routes, "_get_materialized_keyword_source", lambda: FakeMaterializedKeywordSource())

    with pytest.raises(HTTPException) as exc:
        report_routes.keyword_calls_report(
            "missing",
            ReportFiltersQuery(),
            pagination=PaginationQuery(limit=50, offset=0),
            sorting=KeywordCallsSortQuery(),
        )

    assert exc.value.status_code == 404


def test_keyword_drilldown_route_requires_materialized_matches(monkeypatch):
    with pytest.raises(HTTPException) as exc:
        monkeypatch.setattr(
            report_routes,
            "_get_materialized_keyword_source",
            lambda: (_ for _ in ()).throw(HTTPException(status_code=409, detail="Keyword matches are not materialized yet")),
        )
        report_routes.keyword_calls_report(
            "delivery",
            ReportFiltersQuery(),
            pagination=PaginationQuery(limit=50, offset=0),
            sorting=KeywordCallsSortQuery(),
        )

    assert exc.value.status_code == 409
