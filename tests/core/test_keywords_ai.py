from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api.routes import keywords_ai as keywords_ai_routes
from api.schemas import KeywordCatalogAnalysisRequest
from core.keywords_ai import prepare_keyword_catalog_analysis_input
from domain.keywords import KeywordDefinition


class FakeKeywordSource:
    source_name = "fake-keywords"

    def __init__(self, items: list[KeywordDefinition]):
        self.items = items

    def list_keywords(self):
        return list(self.items)

    def close(self):
        return None


class FakeReportingSource:
    source_name = "fake-reporting"

    def __init__(self, records):
        self.records = records

    def iter_call_records(self, filters):
        return iter(self.records)

    def close(self):
        return None


class FakeAnalysisStore:
    def __init__(self):
        self.saved = []
        self.analyses = [
            {
                "analysis_id": "11111111-1111-1111-1111-111111111111",
                "keyword_source": "postgres",
                "reporting_source": "postgres",
                "ai_model": "test-model",
                "ai_summary": "summary",
                "analyzed_keywords": 2,
                "total_candidates_before_limit": 2,
                "truncated": False,
                "created_at": "2026-03-20T12:00:00+00:00",
            }
        ]
        self.analysis_detail = {
            "analysis_id": "11111111-1111-1111-1111-111111111111",
            "keyword_source": "postgres",
            "reporting_source": "postgres",
            "ai_model": "test-model",
            "ai_summary": "summary",
            "analyzed_keywords": 2,
            "total_candidates_before_limit": 2,
            "truncated": False,
            "request": {},
            "analysis_input": {},
            "ai_analysis": {"summary": "summary", "groups": []},
            "created_at": "2026-03-20T12:00:00+00:00",
            "items": {"group": []},
        }

    def save_analysis(self, **kwargs):
        self.saved.append(kwargs)
        return {
            "analysis_id": "11111111-1111-1111-1111-111111111111",
            "created_at": "2026-03-20T12:00:00+00:00",
            "stored_items": 4,
        }

    def list_analyses(self, limit=50):
        return self.analyses[:limit]

    def get_analysis(self, analysis_id: str):
        if analysis_id == self.analysis_detail["analysis_id"]:
            return self.analysis_detail
        return None

    def close(self):
        return None


def _record(call_id: str, summary: str):
    return type(
        "Record",
        (),
        {
            "call_id": call_id,
            "summary": summary,
            "key_questions": [],
            "objections": [],
            "manager_id": "sales_001",
            "manager_name": "Manager 1",
            "role": "sales",
            "direction": "incoming",
            "spam_probability": 0.1,
            "effective_call": True,
            "intent": "consultation",
            "outcome": "sale",
            "audio_seconds": 10.0,
            "call_date": "20260320",
            "src_number": "1",
            "dst_number": "2",
        },
    )()


def test_prepare_keyword_catalog_analysis_input_filters_and_includes_stats():
    keyword_source = FakeKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary"],
                is_active=True,
            ),
            KeywordDefinition(
                keyword_id="inactive_keyword",
                label="Inactive Keyword",
                category="general",
                terms=["inactive"],
                match_fields=["summary"],
                is_active=False,
            ),
        ]
    )
    reporting_source = FakeReportingSource(
        [
            _record("call-1", "delivery delayed"),
            _record("call-2", "delivery issue"),
        ]
    )

    result = prepare_keyword_catalog_analysis_input(
        keyword_source=keyword_source,
        reporting_source=reporting_source,
        include_inactive=False,
        include_match_stats=True,
        max_keywords=10,
        spam_threshold=0.7,
    )

    assert result["analyzed_keywords"] == 1
    assert result["keywords"][0]["keyword_id"] == "delivery"
    assert result["keywords"][0]["matched_calls"] == 2
    assert result["keywords"][0]["total_matches"] == 2


def test_keyword_catalog_analysis_route_returns_ai_grouping(monkeypatch):
    keyword_source = FakeKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary"],
                is_active=True,
            ),
            KeywordDefinition(
                keyword_id="shipment",
                label="Shipment",
                category="logistics",
                terms=["shipment"],
                match_fields=["summary"],
                is_active=True,
            ),
        ]
    )
    reporting_source = FakeReportingSource([_record("call-1", "delivery and shipment")])
    analysis_store = FakeAnalysisStore()

    class FakeLlm:
        def __init__(self, config):
            self.config = config

        def analyze_keyword_catalog(self, keywords, max_groups=20):
            return {
                "summary": "Two overlapping logistics keywords detected.",
                "groups": [
                    {
                        "group_label": "Delivery / Shipment",
                        "theme": "logistics overlap",
                        "keywords": ["delivery", "shipment"],
                        "primary_keyword_id": "delivery",
                        "suggested_category": "logistics",
                        "suggested_shared_terms": ["delivery", "shipment"],
                        "suggested_actions": [
                            {
                                "type": "merge",
                                "keyword_id": "shipment",
                                "target_keyword_id": "delivery",
                                "suggested_label": "",
                                "suggested_terms": ["shipment"],
                                "reason": "The concepts overlap strongly.",
                            }
                        ],
                        "rationale": "Both keywords describe the same delivery topic.",
                    }
                ],
                "ungrouped_keyword_ids": [],
                "global_recommendations": ["Merge overlapping logistics aliases."],
            }

    monkeypatch.setattr(keywords_ai_routes, "_get_keyword_source", lambda: keyword_source)
    monkeypatch.setattr(keywords_ai_routes, "_get_reporting_source", lambda: reporting_source)
    monkeypatch.setattr(keywords_ai_routes, "_get_keyword_ai_analysis_store", lambda: analysis_store)
    monkeypatch.setattr(keywords_ai_routes, "load_app_config", lambda: SimpleNamespace())
    monkeypatch.setattr(keywords_ai_routes, "OllamaLlm", FakeLlm)

    response = keywords_ai_routes.analyze_keyword_catalog(KeywordCatalogAnalysisRequest())

    assert response["analyzed_keywords"] == 2
    assert response["ai_analysis"]["groups"][0]["primary_keyword_id"] == "delivery"
    assert response["ai_analysis"]["groups"][0]["suggested_actions"][0]["type"] == "merge"
    assert response["analysis_history"]["analysis_id"] == "11111111-1111-1111-1111-111111111111"
    assert analysis_store.saved[0]["ai_model"] is None


def test_keyword_catalog_analysis_history_routes(monkeypatch):
    analysis_store = FakeAnalysisStore()

    monkeypatch.setattr(keywords_ai_routes, "_get_required_keyword_ai_analysis_store", lambda: analysis_store)

    analyses = keywords_ai_routes.list_keyword_analyses(limit=10)
    detail = keywords_ai_routes.get_keyword_analysis("11111111-1111-1111-1111-111111111111")

    assert analyses["returned"] == 1
    assert analyses["analyses"][0]["analysis_id"] == "11111111-1111-1111-1111-111111111111"
    assert detail["analysis_id"] == "11111111-1111-1111-1111-111111111111"
    assert detail["ai_analysis"]["summary"] == "summary"


def test_keyword_catalog_analysis_fails_loudly_on_invalid_yaml(monkeypatch, tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text("keywords: [", encoding="utf-8")

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(keywords_ai_routes, "KEYWORDS_CONFIG", keywords_path)

    with pytest.raises(HTTPException) as exc:
        keywords_ai_routes.analyze_keyword_catalog(KeywordCatalogAnalysisRequest(include_match_stats=False))

    assert exc.value.status_code == 500
    assert "Invalid keyword YAML" in exc.value.detail
