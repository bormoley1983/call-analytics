from __future__ import annotations

from api.schemas import KeywordGenerationRequest
from core.keywords_generate import generate_keyword_candidates, publish_generated_keywords
from domain.keywords import KeywordDefinition
from domain.reporting import ReportCallRecord, ReportFilters


class FakeReportingSource:
    def __init__(self, records: list[ReportCallRecord]):
        self.records = records

    def iter_call_records(self, filters: ReportFilters):
        return iter([record for record in self.records if filters.matches_record(record)])

    def close(self):
        return None


class FakeKeywordSource:
    def __init__(self, initial: list[KeywordDefinition] | None = None):
        self.items = {item.keyword_id: item for item in (initial or [])}

    def list_keywords(self):
        return list(self.items.values())

    def get_keyword(self, keyword_id: str):
        return self.items.get(keyword_id)

    def upsert_keyword(self, keyword: KeywordDefinition):
        self.items[keyword.keyword_id] = keyword
        return keyword

    def close(self):
        return None


def _record(call_id: str, summary: str, key_questions: list[str] | None = None) -> ReportCallRecord:
    return ReportCallRecord(
        call_id=call_id,
        manager_id="m1",
        manager_name="Manager",
        role="sales",
        direction="incoming",
        spam_probability=0.1,
        effective_call=True,
        intent="intent",
        outcome="outcome",
        summary=summary,
        audio_seconds=10.0,
        call_date="20260320",
        src_number="1",
        dst_number="2",
        key_questions=key_questions or [],
        objections=[],
    )


def test_keyword_generation_request_defaults_to_effective_only_without_dates():
    req = KeywordGenerationRequest()

    assert req.date_from is None
    assert req.date_to is None
    assert req.effective_only is True


def test_generate_keyword_candidates_from_analysis_texts():
    reporting = FakeReportingSource(
        [
            _record("call-1", "Customer asks refund for delayed delivery", ["Refund status update?"]),
            _record("call-2", "Refund needed due to wrong shipment", ["Can I get refund?"]),
            _record("call-3", "Delivery delayed again", ["Where is delivery"]),
        ]
    )
    keywords = FakeKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary", "key_questions"],
                is_active=True,
            )
        ]
    )

    result = generate_keyword_candidates(
        reporting_source=reporting,
        keyword_source=keywords,
        filters=ReportFilters(),
        include_summary=True,
        include_key_questions=True,
        include_objections=False,
        min_token_length=4,
        max_ngram_words=1,
        min_support_calls=2,
        min_total_matches=2,
        max_candidates=20,
        exclude_existing_terms=True,
    )

    assert result["processed_calls"] == 3
    assert result["candidate_count"] >= 1
    phrases = {item["phrase"] for item in result["candidates"]}
    assert "refund" in phrases
    assert "delivery" not in phrases


def test_publish_generated_keywords_creates_and_skips_existing_terms():
    keywords = FakeKeywordSource(
        [
            KeywordDefinition(
                keyword_id="delivery",
                label="Delivery",
                category="logistics",
                terms=["delivery"],
                match_fields=["summary", "key_questions"],
                is_active=True,
            )
        ]
    )
    result = publish_generated_keywords(
        keyword_source=keywords,
        candidates=[
            {"phrase": "refund"},
            {"phrase": "delivery"},
        ],
        default_category="generated",
        default_match_fields=["summary", "key_questions"],
        default_is_active=False,
    )

    assert result["created"] == 1
    assert result["updated"] == 0
    assert result["skipped_existing_terms"] == 1
    created_id = result["created_keyword_ids"][0]
    created_keyword = keywords.get_keyword(created_id)
    assert created_keyword is not None
    assert created_keyword.terms == ["refund"]
    assert created_keyword.is_active is False


def test_publish_generated_keywords_updates_existing_keyword_when_keyword_id_matches():
    keywords = FakeKeywordSource(
        [
            KeywordDefinition(
                keyword_id="gen_custom",
                label="Refund",
                category="generated",
                terms=["refund"],
                match_fields=["summary"],
                is_active=False,
            )
        ]
    )
    result = publish_generated_keywords(
        keyword_source=keywords,
        candidates=[{"phrase": "return", "keyword_id": "gen_custom"}],
        default_category="generated",
        default_match_fields=["summary"],
        default_is_active=False,
    )

    assert result["created"] == 0
    assert result["updated"] == 1
    updated = keywords.get_keyword("gen_custom")
    assert updated is not None
    assert sorted(updated.terms) == ["refund", "return"]
