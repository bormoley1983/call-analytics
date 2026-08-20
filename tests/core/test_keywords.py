import copy
import json
from datetime import datetime, timezone
from typing import Any

import pytest
import yaml
from fastapi import HTTPException

from adapters.keywords_postgres import PostgresKeywordSource
from adapters.keywords_yaml import YamlKeywordSource
from adapters.reporting_json import JsonReportingSource
import api.deps as api_deps
from api.routes import keywords as keyword_routes
from api.routes import keywords_generation as keyword_generation_routes
from api.routes import reports as report_routes
from api.schemas import (
    KeywordCallSortBy,
    KeywordCallsSortQuery,
    KeywordGenerationBootstrapRequest,
    KeywordManagersSortBy,
    KeywordManagersSortQuery,
    KeywordsSortQuery,
    KeywordSyncRequest,
    KeywordUpsertRequest,
    PaginationQuery,
    ReportFiltersQuery,
    SortOrder,
)
from core.keywords_materialize import materialize_call_keywords
from core.keywords_service import build_keywords_report, list_keywords
from core.keywords_sync import sync_keywords_to_postgres
from domain import config as domain_config
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
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


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
    monkeypatch.setattr(
        report_routes,
        "_get_reporting_source",
        lambda: JsonReportingSource(analysis_dir),
    )
    monkeypatch.setattr(
        report_routes, "_get_keyword_source", lambda: YamlKeywordSource(keywords_path)
    )
    monkeypatch.setattr(
        keyword_routes, "_get_keyword_source", lambda: YamlKeywordSource(keywords_path)
    )

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
    # Path constants are now getters; patch the getter where it is looked up.
    monkeypatch.setattr(api_deps, "get_keywords_config", lambda: keywords_path)

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
    # Path constants are now getters; patch the getter where it is looked up.
    monkeypatch.setattr(api_deps, "get_keywords_config", lambda: keywords_path)
    monkeypatch.setattr(
        report_routes,
        "_get_reporting_source",
        lambda: JsonReportingSource(analysis_dir),
    )

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

    def mark_materialization_completed(
        self, processed_calls, matched_calls, stored_rows
    ):
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


def test_keywords_bootstrap_generates_publishes_and_materializes(monkeypatch):
    keyword_source = FakeWritableKeywordSource()
    reporting_source = FakeReportingSource(
        [
            type(
                "Record",
                (),
                {
                    "call_id": "call-1",
                    "summary": "delivery issue and refund request",
                    "key_questions": ["Where is delivery?", "Can I get refund?"],
                    "objections": [],
                    "manager_id": "sales_001",
                    "spam_probability": 0.1,
                    "effective_call": True,
                    "intent": "консультація",
                    "outcome": "продаж",
                    "call_datetime": datetime(2026, 7, 25, tzinfo=timezone.utc),
                },
            )(),
            type(
                "Record",
                (),
                {
                    "call_id": "call-2",
                    "summary": "refund status and delayed delivery",
                    "key_questions": ["Refund status?"],
                    "objections": [],
                    "manager_id": "sales_002",
                    "spam_probability": 0.1,
                    "effective_call": True,
                    "intent": "консультація",
                    "outcome": "продаж",
                    "call_datetime": datetime(2026, 7, 25, tzinfo=timezone.utc),
                },
            )(),
        ]
    )

    monkeypatch.setattr(
        keyword_generation_routes,
        "_get_postgres_reporting_source",
        lambda: reporting_source,
    )
    monkeypatch.setattr(
        keyword_generation_routes,
        "_get_postgres_keyword_source",
        lambda: keyword_source,
    )
    monkeypatch.setattr(
        keyword_generation_routes,
        "_run_keyword_ai_analysis_once",
        lambda trigger, skip_if_empty=False: {"trigger": trigger},
    )

    response: dict[str, Any] = keyword_generation_routes.bootstrap_keywords(
        KeywordGenerationBootstrapRequest(
            min_token_length=4,
            max_ngram_words=1,
            min_support_calls=2,
            min_total_matches=2,
            max_candidates=20,
            materialize_after_publish=True,
            run_ai_analysis_after_publish=True,
        )
    )

    assert response["generation"]["candidate_count"] >= 1
    assert response["publish"]["created"] >= 1
    assert response["materialized"] is True
    assert response["materialize"]["processed_calls"] == 2
    assert response["keyword_ai_analysis"] == {"trigger": "keywords-bootstrap"}


def test_keywords_bootstrap_skips_materialize_and_ai_without_changes(monkeypatch):
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
    reporting_source = FakeReportingSource(
        [
            type(
                "Record",
                (),
                {
                    "call_id": "call-1",
                    "summary": "delivery only",
                    "key_questions": ["Where is delivery?"],
                    "objections": [],
                    "manager_id": "sales_001",
                    "spam_probability": 0.1,
                    "effective_call": True,
                    "intent": "консультація",
                    "outcome": "продаж",
                    "call_datetime": datetime(2026, 7, 25, tzinfo=timezone.utc),
                },
            )(),
        ]
    )

    monkeypatch.setattr(
        keyword_generation_routes,
        "_get_postgres_reporting_source",
        lambda: reporting_source,
    )
    monkeypatch.setattr(
        keyword_generation_routes,
        "_get_postgres_keyword_source",
        lambda: keyword_source,
    )
    monkeypatch.setattr(
        keyword_generation_routes,
        "_run_keyword_ai_analysis_once",
        lambda trigger, skip_if_empty=False: {"trigger": trigger},
    )

    response: dict[str, Any] = keyword_generation_routes.bootstrap_keywords(
        KeywordGenerationBootstrapRequest(
            min_token_length=4,
            max_ngram_words=1,
            min_support_calls=2,
            min_total_matches=1,
            max_candidates=20,
            exclude_existing_terms=True,
            materialize_after_publish=True,
            run_ai_analysis_after_publish=True,
        )
    )

    assert response["publish"]["created"] == 0
    assert response["publish"]["updated"] == 0
    assert response["materialized"] is False
    assert response["keyword_ai_analysis"] is None


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

    upserted = keyword_routes.upsert_keyword(
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

    assert upserted["keyword_id"] == "refund"
    assert updated["label"] == "Refund Updated"
    assert source.get_keyword("refund") is None


def test_keyword_management_routes_enforce_read_only_without_postgres(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    with pytest.raises(HTTPException) as exc:
        keyword_routes.upsert_keyword(
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

    monkeypatch.setattr(
        keyword_routes,
        "_get_yaml_keyword_source",
        lambda strict=False: YamlKeywordSource(keywords_path, strict=strict),
    )
    monkeypatch.setattr(
        keyword_routes, "_get_writable_keyword_source", lambda: postgres_source
    )
    monkeypatch.setattr(
        keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result
    )

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

    monkeypatch.setattr(
        keyword_routes,
        "_get_yaml_keyword_source",
        lambda strict=False: YamlKeywordSource(keywords_path, strict=strict),
    )
    monkeypatch.setattr(
        keyword_routes, "_get_writable_keyword_source", lambda: postgres_source
    )
    monkeypatch.setattr(
        keyword_routes, "_get_postgres_reporting_source", lambda: reporting_source
    )
    monkeypatch.setattr(
        keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result
    )

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
        lambda strict=False: YamlKeywordSource(
            tmp_path / "missing.yaml", strict=strict
        ),
    )
    monkeypatch.setattr(
        keyword_routes, "_get_writable_keyword_source", lambda: postgres_source
    )
    monkeypatch.setattr(
        keyword_routes, "_append_keyword_ai_analysis", lambda result, trigger: result
    )

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
        FakeReportingSource(records),  # type: ignore[arg-type]
        keyword_source,  # type: ignore[arg-type]
        keyword_source,
        state_store=keyword_source,
    )

    assert response["processed_calls"] == 1
    assert response["matched_calls"] == 1
    assert response["stored_rows"] == 2
    assert keyword_source.last_replaced[0] == "call-1"
    assert {row["keyword_id"] for row in keyword_source.last_replaced[1]} == {
        "delivery",
        "refund",
    }
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
        FakeReportingSourceFromAnalyses(analyses),  # type: ignore[arg-type]
        keyword_source,  # type: ignore[arg-type]
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

        def build_materialized_keywords_report(  # type: ignore[override]
            self, filters, spam_threshold, sort_by, order
        ):
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

    monkeypatch.setattr(
        report_routes, "_get_reporting_source", lambda: FakePostgresReportingSource()
    )
    monkeypatch.setattr(
        report_routes, "_get_keyword_source", lambda: FakeMaterializedKeywordSource()
    )

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

        def build_keyword_calls_report(  # type: ignore[override]
            self, keyword_id, filters, spam_threshold, limit, offset, sort_by, order
        ):
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
                "points": [
                    {"call_date": "20241112", "matched_calls": 1, "total_matches": 2}
                ],
            }

        def build_keyword_managers_report(  # type: ignore[override]
            self, keyword_id, filters, spam_threshold, sort_by, order
        ):
            return {
                "keyword_id": keyword_id,
                "report_data_source": "postgres_materialized",
                "sort_by": sort_by,
                "order": order,
                "managers": [
                    {"manager_id": "sales_001", "matched_calls": 1, "total_matches": 2}
                ],
            }

        def close(self):
            return None

    monkeypatch.setattr(
        report_routes,
        "_get_materialized_keyword_source",
        lambda: FakeMaterializedKeywordSource(),
    )

    calls = report_routes.keyword_calls_report(
        "delivery",
        ReportFiltersQuery(),
        pagination=PaginationQuery(limit=25, offset=0),
        sorting=KeywordCallsSortQuery(
            sort_by=KeywordCallSortBy.match_count, order=SortOrder.asc
        ),
    )
    trend = report_routes.keyword_trend_report("delivery", ReportFiltersQuery())
    managers = report_routes.keyword_managers_report(
        "delivery",
        ReportFiltersQuery(),
        sorting=KeywordManagersSortQuery(
            sort_by=KeywordManagersSortBy.manager_name, order=SortOrder.asc
        ),
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

    monkeypatch.setattr(
        report_routes,
        "_get_materialized_keyword_source",
        lambda: FakeMaterializedKeywordSource(),
    )

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
            lambda: (_ for _ in ()).throw(
                HTTPException(
                    status_code=409, detail="Keyword matches are not materialized yet"
                )
            ),
        )
        report_routes.keyword_calls_report(
            "delivery",
            ReportFiltersQuery(),
            pagination=PaginationQuery(limit=50, offset=0),
            sorting=KeywordCallsSortQuery(),
        )

    assert exc.value.status_code == 409


# ---------------------------------------------------------------------------
# keywords_service - _normalize, _record_texts, _match_keyword, _include_record
# ---------------------------------------------------------------------------


def _make_record(**kwargs):
    from domain.reporting import ReportCallRecord

    defaults: dict[str, object] = dict(
        call_id="call-1",
        manager_id="sales_001",
        manager_name="Manager 1",
        role="sales",
        direction="incoming",
        spam_probability=0.1,
        effective_call=True,
        intent="консультація",
        outcome="продаж",
        summary="Client asked about delivery options.",
        audio_seconds=120.0,
        call_datetime=datetime(2026, 7, 25, tzinfo=timezone.utc),
        src_number="+380501234567",
        dst_number="+380441234567",
        key_questions=["Where is my order?"],
        objections=["Delivery is too expensive"],
    )
    defaults.update(kwargs)
    return ReportCallRecord(**defaults)  # type: ignore[arg-type]


def _make_keyword(**kwargs):
    defaults: dict[str, object] = dict(
        keyword_id="delivery",
        label="Delivery",
        category="logistics",
        terms=["delivery", "order"],
        match_fields=["summary", "key_questions", "objections"],
        is_active=True,
    )
    defaults.update(kwargs)
    return KeywordDefinition(**defaults)  # type: ignore[arg-type]


def test_normalize_case_folds_and_strips():
    from core.keywords_service import _normalize

    assert _normalize("  Hello World  ") == "hello world"
    assert _normalize("") == ""
    assert _normalize("Привіт") == "привіт"


def test_record_texts_selects_fields():
    from core.keywords_service import _record_texts

    record = _make_record(summary="test summary", key_questions=["Q1?"])

    result = _record_texts(record, ["summary"])
    assert result["summary"] == ["test summary"]

    result_kq = _record_texts(record, ["key_questions"])
    assert result_kq["key_questions"] == ["Q1?"]


def test_record_texts_filters_empty_strings():
    from core.keywords_service import _record_texts

    record = _make_record(key_questions=["Q1?", "", "Q3?"])
    result = _record_texts(record, ["key_questions"])
    assert result["key_questions"] == ["Q1?", "Q3?"]


def test_match_keyword_in_summary():
    from core.keywords_service import _match_keyword

    record = _make_record(summary="delivery issue")
    keyword = _make_keyword(terms=["delivery"], match_fields=["summary"])
    matches = _match_keyword(record, keyword)
    assert len(matches) == 1
    assert matches[0]["field"] == "summary"


def test_match_keyword_case_insensitive():
    from core.keywords_service import _match_keyword

    record = _make_record(summary="DELIVERY is great")
    keyword = _make_keyword(terms=["delivery"], match_fields=["summary"])
    assert len(_match_keyword(record, keyword)) == 1


def test_match_keyword_no_match_when_absent():
    from core.keywords_service import _match_keyword

    record = _make_record(summary="completely unrelated text")
    keyword = _make_keyword(terms=["refund"], match_fields=["summary"])
    assert _match_keyword(record, keyword) == []


def test_match_keyword_multiple_terms_same_field():
    from core.keywords_service import _match_keyword

    record = _make_record(summary="delivery and order update")
    keyword = _make_keyword(terms=["delivery", "order"], match_fields=["summary"])
    assert len(_match_keyword(record, keyword)) == 2


def test_match_keyword_across_fields():
    from core.keywords_service import _match_keyword

    record = _make_record(
        summary="delivery issue", key_questions=["Order status?"]
    )
    keyword = _make_keyword(
        terms=["delivery", "order"], match_fields=["summary", "key_questions"]
    )
    assert len(_match_keyword(record, keyword)) == 2


def test_include_record_effective_only_filter():
    from core.keywords_service import _include_record

    record_on = _make_record(effective_call=True)
    record_off = _make_record(effective_call=False)
    filters = ReportFilters(effective_only=True)

    assert _include_record(record_on, filters, 0.7) is True
    assert _include_record(record_off, filters, 0.7) is False


def test_include_record_spam_only_filter():
    from core.keywords_service import _include_record

    spam_record = _make_record(spam_probability=0.9)
    clean_record = _make_record(spam_probability=0.2)
    filters = ReportFilters(spam_only=True)

    assert _include_record(spam_record, filters, 0.7) is True
    assert _include_record(clean_record, filters, 0.7) is False


# ---------------------------------------------------------------------------
# keywords_service - _finalize_keywords_report & dispatch logic
# ---------------------------------------------------------------------------


def test_finalize_keywords_report_structure():
    from core.keywords_service import _finalize_keywords_report

    buckets = {
        "delivery": {
            "keyword_id": "delivery",
            "label": "Delivery",
            "category": "logistics",
            "terms": ["delivery"],
            "match_fields": ["summary"],
            "matched_calls": 5,
            "total_matches": 10,
            "matched_managers": {"sales_001", "sales_002"},
            "intents": {"консультація": 3},
            "outcomes": {"продаж": 5},
        }
    }

    result = _finalize_keywords_report(
        buckets=buckets,
        reporting_source=type("R", (), {"source_name": "fake"})(),
        keyword_source=type("K", (), {"source_name": "fake"})(),
        filters=ReportFilters(),
        sort_by="matched_calls",
        order="desc",
    )

    assert result["total_keywords"] == 1
    kw = result["keywords"][0]
    assert kw["keyword_id"] == "delivery"
    assert kw["matched_managers"] == 2


def test_finalize_sorts_by_matched_calls_descending():
    from core.keywords_service import _finalize_keywords_report

    buckets = {
        "a": {
            "keyword_id": "a",
            "label": "A",
            "category": "cat",
            "terms": ["a"],
            "match_fields": ["summary"],
            "matched_calls": 10,
            "total_matches": 10,
            "matched_managers": set(),
            "intents": {},
            "outcomes": {},
        },
        "b": {
            "keyword_id": "b",
            "label": "B",
            "category": "cat",
            "terms": ["b"],
            "match_fields": ["summary"],
            "matched_calls": 20,
            "total_matches": 20,
            "matched_managers": set(),
            "intents": {},
            "outcomes": {},
        },
    }

    result = _finalize_keywords_report(
        buckets=buckets,
        reporting_source=type("R", (), {"source_name": "fake"})(),
        keyword_source=type("K", (), {"source_name": "fake"})(),
        filters=ReportFilters(),
        sort_by="matched_calls",
        order="desc",
    )

    assert result["keywords"][0]["keyword_id"] == "b"
    assert result["keywords"][1]["keyword_id"] == "a"


def test_build_keywords_report_dispatches_to_sql_path():
    """When reporting_source has build_keywords_report_data, use SQL path."""

    class SqlReportingSource:
        source_name = "sql_fake"

        def build_overall_report_data(self, filters, spam_threshold):
            return {}

        def build_managers_report_data(
            self, filters, spam_threshold, sort_by="total_calls", order="desc"
        ):
            return {}

        def build_customers_report_data(
            self, filters, spam_threshold, sort_by="total_calls", order="desc"
        ):
            return {}

        def build_keywords_report_data(self, **kwargs):
            return [
                {
                    "keyword_id": "delivery",
                    "matched_calls": 3,
                    "total_matches": 5,
                    "top_managers": [("sales_001", 2)],
                    "top_intents": [("консультація", 3)],
                    "top_outcomes": [("продаж", 3)],
                }
            ]

        def iter_call_records(self, filters):
            return []

        def close(self):
            pass

    class FakeKeywordSource:
        source_name = "fake"

        def list_keywords(self):
            return [_make_keyword()]

        def close(self):
            pass

    result = build_keywords_report(
        reporting_source=SqlReportingSource(),
        keyword_source=FakeKeywordSource(),
        filters=ReportFilters(),
        spam_threshold=0.7,
    )

    assert result["total_keywords"] == 1
    assert result["keywords"][0]["matched_calls"] == 3


def test_build_keywords_report_dispatches_to_iterative_path():
    """When reporting_source lacks build_keywords_report_data, use iterative path."""

    class IterReportingSource:
        source_name = "iter_fake"

        def iter_call_records(self, filters):
            return [_make_record(summary="delivery issue")]

        def close(self):
            pass

    class FakeKeywordSource:
        source_name = "fake"

        def list_keywords(self):
            return [_make_keyword(terms=["delivery"])]

        def close(self):
            pass

    result = build_keywords_report(
        reporting_source=IterReportingSource(),
        keyword_source=FakeKeywordSource(),
        filters=ReportFilters(),
        spam_threshold=0.7,
    )

    assert result["total_keywords"] == 1
    assert result["keywords"][0]["matched_calls"] == 1


# ---------------------------------------------------------------------------
# keywords_materialize - batching with batch_replace_call_keyword_matches
# ---------------------------------------------------------------------------


class FakeBatchKeywordStore:
    """Keyword store with batch_replace_call_keyword_matches."""

    def __init__(self):
        self.batch_calls: list[list[tuple[str, list[dict]]]] = []
        self.individual_calls: list[tuple[str, list[dict]]] = []
        self.materialization_state: dict | None = None

    def batch_replace_call_keyword_matches(
        self, batches: list[tuple[str, list[dict]]]
    ):
        self.batch_calls.append(list(batches))

    def replace_call_keyword_matches(self, call_id: str, rows: list[dict]):
        self.individual_calls.append((call_id, rows))

    def is_materialized(self):
        return self.materialization_state is not None

    def mark_materialization_completed(
        self, processed_calls: int, matched_calls: int, stored_rows: int
    ):
        self.materialization_state = {
            "processed_calls": processed_calls,
            "matched_calls": matched_calls,
            "stored_rows": stored_rows,
        }

    def close(self):
        pass


class FakeNoBatchKeywordStore:
    """Keyword store WITHOUT batch_replace_call_keyword_matches."""

    def __init__(self):
        self.individual_calls: list[tuple[str, list[dict]]] = []
        self.materialization_state: dict | None = None

    def replace_call_keyword_matches(self, call_id: str, rows: list[dict]):
        self.individual_calls.append((call_id, rows))

    def is_materialized(self):
        return self.materialization_state is not None

    def mark_materialization_completed(
        self, processed_calls: int, matched_calls: int, stored_rows: int
    ):
        self.materialization_state = {
            "processed_calls": processed_calls,
            "matched_calls": matched_calls,
            "stored_rows": stored_rows,
        }

    def close(self):
        pass


class FakeReportingSourceForMaterialize:
    def __init__(self, records):
        self.records = records

    def iter_call_records(self, filters):
        return iter(self.records)

    def close(self):
        pass


class FakeKeywordSourceForMaterialize:
    def __init__(self, keywords=None):
        self.keywords = keywords or []

    def list_keywords(self):
        return list(self.keywords)

    def close(self):
        pass


def test_materialize_uses_batch_method_when_available():
    records = [
        _make_record(call_id="call-1", summary="delivery issue"),
        _make_record(call_id="call-2", summary="another delivery problem"),
        _make_record(call_id="call-3", summary="no match here"),
    ]
    store = FakeBatchKeywordStore()

    result = materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert result["processed_calls"] == 3
    assert len(store.batch_calls) >= 1
    assert len(store.individual_calls) == 0


def test_materialize_falls_back_to_individual_when_no_batch():
    records = [
        _make_record(call_id="call-1", summary="delivery issue"),
        _make_record(call_id="call-2", summary="another delivery problem"),
    ]
    store = FakeNoBatchKeywordStore()

    result = materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert result["processed_calls"] == 2
    assert len(store.individual_calls) == 2


def test_materialize_flushes_remaining_batch_at_end():
    records = [_make_record(call_id=f"call-{i}", summary="delivery") for i in range(3)]
    store = FakeBatchKeywordStore()

    materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert len(store.batch_calls) == 1
    total_in_batch = sum(len(b) for b in store.batch_calls)
    assert total_in_batch == 3


def test_materialize_batches_at_batch_size_boundary():
    records = [
        _make_record(call_id=f"call-{i:04d}", summary="delivery issue") for i in range(600)
    ]
    store = FakeBatchKeywordStore()

    materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert len(store.batch_calls) == 2
    assert len(store.batch_calls[0]) == 500
    assert len(store.batch_calls[1]) == 100


def test_materialize_counts_matched_vs_unmatched():
    records = [
        _make_record(call_id="call-1", summary="delivery issue"),
        _make_record(
            call_id="call-2",
            summary="completely unrelated topic",
            key_questions=[],
            objections=[],
        ),
        _make_record(call_id="call-3", summary="another delivery"),
    ]
    store = FakeBatchKeywordStore()

    result = materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert result["processed_calls"] == 3
    assert result["matched_calls"] == 2


def test_materialize_skips_inactive_keywords():
    records = [_make_record(call_id="call-1", summary="delivery issue")]
    store = FakeBatchKeywordStore()

    result = materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize(records),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize(
            [_make_keyword(is_active=False)]
        ),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert result["active_keywords"] == 0
    assert result["matched_calls"] == 0


def test_materialize_empty_records_produces_no_batches():
    store = FakeBatchKeywordStore()

    result = materialize_call_keywords(
        reporting_source=FakeReportingSourceForMaterialize([]),  # type: ignore[arg-type]
        keyword_source=FakeKeywordSourceForMaterialize([_make_keyword()]),  # type: ignore[arg-type]
        keyword_store=store,
        state_store=store,
    )

    assert result["processed_calls"] == 0
    assert len(store.batch_calls) == 0
