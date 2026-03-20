from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from domain.keywords import DEFAULT_MATCH_FIELDS, KeywordDefinition
from domain.reporting import ReportFilters
from ports.keywords import KeywordSource
from ports.reporting import ReportingSource

_TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яІіЇїЄєҐґ][0-9A-Za-zА-Яа-яІіЇїЄєҐґ'\-]*")
_SPACES_RE = re.compile(r"\s+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "with",
    "i",
    "you",
    "we",
    "they",
    "це",
    "цей",
    "ця",
    "ці",
    "цею",
    "та",
    "і",
    "й",
    "або",
    "але",
    "щоб",
    "як",
    "якщо",
    "у",
    "в",
    "на",
    "до",
    "з",
    "за",
    "по",
    "від",
    "для",
    "про",
    "при",
    "не",
    "ні",
    "так",
    "таке",
    "також",
    "що",
    "це",
    "мене",
    "вам",
    "нам",
    "його",
    "її",
    "їх",
    "він",
    "вона",
    "вони",
    "ми",
    "ви",
    "я",
}


@dataclass
class _CandidateStats:
    total_matches: int = 0
    call_ids: set[str] = field(default_factory=set)
    sample_call_ids: list[str] = field(default_factory=list)


def _normalize_text(text: str) -> str:
    return _SPACES_RE.sub(" ", text.casefold()).strip()


def _suggest_label(phrase: str) -> str:
    phrase = phrase.strip()
    if not phrase:
        return phrase
    return phrase[:1].upper() + phrase[1:]


def _generated_keyword_id(phrase: str) -> str:
    digest = hashlib.sha1(phrase.encode("utf-8")).hexdigest()[:12]
    return f"gen_{digest}"


def _candidate_id(phrase: str) -> str:
    digest = hashlib.sha1(phrase.encode("utf-8")).hexdigest()[:12]
    return f"cand_{digest}"


def _tokenize(text: str, min_token_length: int) -> list[str]:
    return [
        token
        for token in (match.group(0).strip("-'") for match in _TOKEN_RE.finditer(text.casefold()))
        if token and len(token) >= min_token_length
    ]


def _iter_phrases(tokens: list[str], max_ngram_words: int) -> list[str]:
    phrases: list[str] = []
    if not tokens:
        return phrases
    max_len = min(max_ngram_words, len(tokens))
    for phrase_len in range(1, max_len + 1):
        for start in range(0, len(tokens) - phrase_len + 1):
            phrase_tokens = tokens[start : start + phrase_len]
            if phrase_tokens[0] in _STOPWORDS or phrase_tokens[-1] in _STOPWORDS:
                continue
            if all(token in _STOPWORDS for token in phrase_tokens):
                continue
            phrase = " ".join(phrase_tokens).strip()
            if phrase and any(ch.isalpha() for ch in phrase):
                phrases.append(phrase)
    return phrases


def _iter_record_texts(
    record: Any,
    include_summary: bool,
    include_key_questions: bool,
    include_objections: bool,
) -> list[str]:
    texts: list[str] = []
    if include_summary and record.summary:
        texts.append(record.summary)
    if include_key_questions:
        texts.extend(item for item in record.key_questions if isinstance(item, str) and item.strip())
    if include_objections:
        texts.extend(item for item in record.objections if isinstance(item, str) and item.strip())
    return texts


def generate_keyword_candidates(
    reporting_source: ReportingSource,
    keyword_source: KeywordSource,
    *,
    filters: ReportFilters,
    include_summary: bool = True,
    include_key_questions: bool = True,
    include_objections: bool = True,
    min_token_length: int = 4,
    max_ngram_words: int = 2,
    min_support_calls: int = 5,
    min_total_matches: int = 5,
    max_candidates: int = 100,
    exclude_existing_terms: bool = True,
    spam_threshold: float = 0.7,
) -> dict[str, Any]:
    selected_match_fields: list[str] = []
    if include_summary:
        selected_match_fields.append("summary")
    if include_key_questions:
        selected_match_fields.append("key_questions")
    if include_objections:
        selected_match_fields.append("objections")
    if not selected_match_fields:
        selected_match_fields = list(DEFAULT_MATCH_FIELDS)

    existing_terms: set[str] = set()
    if exclude_existing_terms:
        for keyword in keyword_source.list_keywords():
            for term in keyword.terms:
                normalized = _normalize_text(str(term))
                if normalized:
                    existing_terms.add(normalized)

    stats: dict[str, _CandidateStats] = {}
    processed_calls = 0
    skipped_existing_term_hits = 0

    for record in reporting_source.iter_call_records(filters):
        if filters.spam_only and record.spam_probability < spam_threshold:
            continue
        if filters.effective_only and not record.effective_call:
            continue
        processed_calls += 1
        texts = _iter_record_texts(
            record,
            include_summary=include_summary,
            include_key_questions=include_key_questions,
            include_objections=include_objections,
        )
        if not texts:
            continue
        for text in texts:
            tokens = _tokenize(text, min_token_length=min_token_length)
            if not tokens:
                continue
            for phrase in _iter_phrases(tokens, max_ngram_words=max_ngram_words):
                normalized_phrase = _normalize_text(phrase)
                if not normalized_phrase:
                    continue
                if normalized_phrase in existing_terms:
                    skipped_existing_term_hits += 1
                    continue
                bucket = stats.setdefault(normalized_phrase, _CandidateStats())
                bucket.total_matches += 1
                if record.call_id not in bucket.call_ids:
                    bucket.call_ids.add(record.call_id)
                    if len(bucket.sample_call_ids) < 5:
                        bucket.sample_call_ids.append(record.call_id)

    candidates: list[dict[str, Any]] = []
    for phrase, bucket in stats.items():
        support_calls = len(bucket.call_ids)
        if support_calls < min_support_calls:
            continue
        if bucket.total_matches < min_total_matches:
            continue
        candidates.append(
            {
                "candidate_id": _candidate_id(phrase),
                "phrase": phrase,
                "support_calls": support_calls,
                "total_matches": bucket.total_matches,
                "sample_call_ids": bucket.sample_call_ids,
                "suggested_keyword_id": _generated_keyword_id(phrase),
                "suggested_label": _suggest_label(phrase),
                "suggested_match_fields": selected_match_fields,
            }
        )

    candidates.sort(key=lambda item: (-item["support_calls"], -item["total_matches"], item["phrase"]))
    candidates = candidates[:max_candidates]

    return {
        "processed_calls": processed_calls,
        "candidate_count": len(candidates),
        "excluded_existing_terms": exclude_existing_terms,
        "skipped_existing_term_hits": skipped_existing_term_hits,
        "selected_match_fields": selected_match_fields,
        "candidates": candidates,
    }


def publish_generated_keywords(
    keyword_source: Any,
    candidates: list[dict[str, Any]],
    *,
    default_category: str,
    default_match_fields: list[str],
    default_is_active: bool,
) -> dict[str, Any]:
    existing_by_id = {keyword.keyword_id: keyword for keyword in keyword_source.list_keywords()}
    existing_term_to_keyword_id: dict[str, str] = {}
    for keyword in existing_by_id.values():
        for term in keyword.terms:
            normalized = _normalize_text(term)
            if normalized:
                existing_term_to_keyword_id[normalized] = keyword.keyword_id

    created_keyword_ids: list[str] = []
    updated_keyword_ids: list[str] = []
    skipped_existing_terms: list[str] = []
    skipped_invalid: list[str] = []

    for raw_candidate in candidates:
        phrase = _normalize_text(str(raw_candidate.get("phrase") or ""))
        if not phrase:
            skipped_invalid.append(str(raw_candidate.get("phrase") or ""))
            continue

        if phrase in existing_term_to_keyword_id:
            skipped_existing_terms.append(phrase)
            continue

        provided_keyword_id = str(raw_candidate.get("keyword_id") or "").strip()
        keyword_id = provided_keyword_id or _generated_keyword_id(phrase)
        existing = existing_by_id.get(keyword_id)

        if existing is None:
            label = str(raw_candidate.get("label") or "").strip() or _suggest_label(phrase)
            category = str(raw_candidate.get("category") or "").strip() or default_category
            match_fields = raw_candidate.get("match_fields") or list(default_match_fields)
            is_active = raw_candidate.get("is_active")
            if is_active is None:
                is_active = default_is_active

            created = keyword_source.upsert_keyword(
                KeywordDefinition(
                    keyword_id=keyword_id,
                    label=label,
                    category=category,
                    terms=[phrase],
                    match_fields=list(match_fields),
                    is_active=bool(is_active),
                )
            )
            created_keyword_ids.append(created.keyword_id)
            existing_by_id[created.keyword_id] = created
            existing_term_to_keyword_id[phrase] = created.keyword_id
            continue

        updated_terms = sorted({*existing.terms, phrase}, key=lambda item: item.casefold())
        label = str(raw_candidate.get("label") or "").strip() or existing.label
        category = str(raw_candidate.get("category") or "").strip() or existing.category
        match_fields = raw_candidate.get("match_fields") or existing.match_fields or list(default_match_fields)
        is_active = existing.is_active if raw_candidate.get("is_active") is None else bool(raw_candidate["is_active"])

        updated = keyword_source.upsert_keyword(
            KeywordDefinition(
                keyword_id=existing.keyword_id,
                label=label,
                category=category,
                terms=updated_terms,
                match_fields=list(match_fields),
                is_active=is_active,
            )
        )
        updated_keyword_ids.append(updated.keyword_id)
        existing_by_id[updated.keyword_id] = updated
        existing_term_to_keyword_id[phrase] = updated.keyword_id

    return {
        "created": len(created_keyword_ids),
        "updated": len(updated_keyword_ids),
        "skipped_existing_terms": len(skipped_existing_terms),
        "skipped_invalid": len(skipped_invalid),
        "created_keyword_ids": created_keyword_ids,
        "updated_keyword_ids": updated_keyword_ids,
        "skipped_existing_term_values": sorted(skipped_existing_terms),
    }
