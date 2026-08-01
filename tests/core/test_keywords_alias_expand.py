from __future__ import annotations

import pytest

from core.keywords_alias_expand import expand_keyword_aliases


class FakeKeywordSource:
    def __init__(self, keyword):
        self.keyword = keyword

    def get_keyword(self, keyword_id):
        if self.keyword and self.keyword.keyword_id == keyword_id:
            return self.keyword
        raise KeyError(keyword_id)

    def close(self):
        pass


class FakeReportingSource:
    def __init__(self, records):
        self.records = records

    def iter_call_records(self, filters):
        return iter(self.records)

    def close(self):
        pass


class FakeLlm:
    def expand_aliases(
        self, *, keyword_id, label, current_terms, evidence_texts, max_aliases
    ):
        return {
            "suggested_aliases": [
                {
                    "phrase": f"alias for {label}",
                    "confidence_score": 0.8,
                    "reason": "test",
                },
            ],
            "ai_model": "fake",
        }


def _make_keyword(keyword_id, label, terms=None, aliases=None):
    from domain.keywords import KeywordDefinition

    return KeywordDefinition(
        keyword_id=keyword_id,
        label=label,
        category="test",
        terms=terms or [label],
        match_fields=["summary"],
        is_active=True,
    )


def _make_record(call_id, summary, key_questions=None, objections=None):
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


def test_expand_aliases_basic():
    keyword = _make_keyword(
        "kw1", "pricing inquiry", terms=["pricing inquiry", "price question"]
    )
    records = [
        _make_record(
            "call1",
            "Customer asked about pricing and discounts",
            key_questions=["What is the price?"],
        )
    ]

    result = expand_keyword_aliases(
        keyword_id="kw1",
        keyword_source=FakeKeywordSource(keyword),
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases=5,
    )

    assert result["keyword_id"] == "kw1"
    assert "pricing inquiry" in result["current_terms"]
    assert len(result["suggested_aliases"]) > 0


def test_expand_aliases_filters_existing():
    """Existing terms should be filtered out of suggestions."""
    keyword = _make_keyword(
        "kw1", "pricing inquiry", terms=["pricing inquiry", "price question"]
    )
    records = [_make_record("call1", "Customer asked about pricing")]

    class DuplicateLlm(FakeLlm):
        def expand_aliases(
            self, *, keyword_id, label, current_terms, evidence_texts, max_aliases
        ):
            # Return an alias that already exists
            return {
                "suggested_aliases": [
                    {"phrase": "pricing inquiry", "confidence_score": 0.9},  # duplicate
                    {"phrase": "new alias", "confidence_score": 0.8},
                ],
                "ai_model": "fake",
            }

    result = expand_keyword_aliases(
        keyword_id="kw1",
        keyword_source=FakeKeywordSource(keyword),
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=DuplicateLlm(),  # type: ignore[arg-type]
        max_aliases=5,
    )

    # Should only have the non-duplicate alias
    phrases = [a.get("phrase", "") for a in result["suggested_aliases"]]
    assert "pricing inquiry" not in phrases
    assert "new alias" in phrases


def test_expand_aliases_keyword_not_found():
    keyword = _make_keyword("kw1", "some phrase", terms=["some phrase"])
    records: list = []

    with pytest.raises(KeyError):
        expand_keyword_aliases(
            keyword_id="nonexistent",
            keyword_source=FakeKeywordSource(keyword),
            reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
            llm=FakeLlm(),  # type: ignore[arg-type]
            max_aliases=5,
        )


def test_expand_aliases_no_evidence():
    """When no evidence is available, should still work but with empty evidence."""
    keyword = _make_keyword("kw1", "test phrase")
    records = [_make_record("call1", "")]  # Empty summary

    result = expand_keyword_aliases(
        keyword_id="kw1",
        keyword_source=FakeKeywordSource(keyword),
        reporting_source=FakeReportingSource(records),  # type: ignore[arg-type]
        llm=FakeLlm(),  # type: ignore[arg-type]
        max_aliases=3,
    )

    assert result["keyword_id"] == "kw1"
