from __future__ import annotations

import logging
from typing import Any

from core.keywords_service import _match_keyword
from domain.keywords import KeywordDefinition
from domain.reporting import ReportFilters
from ports.keywords import KeywordLookupSource
from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)


def expand_keyword_aliases(
    keyword_id: str,
    keyword_source: KeywordLookupSource,
    reporting_source: ReportingSource,
    llm: LlmPort,
    *,
    max_aliases: int = 5,
    filters: ReportFilters | None = None,
) -> dict[str, Any]:
    """Suggest conservative aliases for a single keyword.

    Fetches the keyword definition, gathers recent evidence from analyses,
    calls the LLM for alias suggestions, and returns the result.
    """
    # Fetch keyword definition
    keyword = keyword_source.get_keyword(keyword_id)
    if keyword is None:
        raise ValueError(f"Keyword '{keyword_id}' not found in catalog")

    current_terms = list(keyword.terms) if keyword.terms else []
    label = keyword.label

    # Gather evidence only from records that actually match this
    # keyword (previously the first 50 texts of ANY records were used).
    evidence_texts = _gather_evidence_texts(
        reporting_source, keyword, filters=filters
    )

    logger.info(
        "Expanding aliases for keyword %s: current_terms=%d evidence_texts=%d",
        keyword_id,
        len(current_terms),
        len(evidence_texts),
    )

    # Call LLM for alias suggestions
    llm_result: dict[str, Any] = llm.expand_aliases(
        keyword_id=keyword_id,
        label=label,
        current_terms=current_terms,
        evidence_texts=evidence_texts,
        max_aliases=max_aliases,
    )

    suggested_aliases = llm_result.get("suggested_aliases", [])

    # Filter out aliases that are already in current terms
    existing_set = {t.casefold() for t in current_terms}
    filtered_aliases = [
        a
        for a in suggested_aliases
        if a.get("phrase", "").casefold() not in existing_set
    ]

    logger.info(
        "Alias expansion for keyword %s: llm_suggestions=%d after_filter=%d",
        keyword_id,
        len(suggested_aliases),
        len(filtered_aliases),
    )

    return {
        "keyword_id": keyword_id,
        "current_terms": current_terms,
        "suggested_aliases": filtered_aliases,
        "evidence_texts_count": len(evidence_texts),
    }


def _gather_evidence_texts(
    reporting_source: ReportingSource,
    keyword: KeywordDefinition,
    *,
    filters: ReportFilters | None = None,
) -> list[str]:
    """Gather recent matched texts from analyses for evidence.

    Only records that match the keyword (via the shared
    ``_match_keyword`` helper) contribute evidence texts — previously the
    first 50 texts of any records were taken regardless of relevance.
    """
    effective_filters = filters or ReportFilters()
    evidence: list[str] = []
    seen: set[str] = set()

    matched_records = 0
    scanned_records = 0
    for record in reporting_source.iter_call_records(effective_filters):
        scanned_records += 1
        matches = _match_keyword(record, keyword)
        if not matches:
            continue
        matched_records += 1
        # Collect the matched text values (deduplicated).
        for match in matches:
            text = match["text"]
            if text and text not in seen:
                evidence.append(text)
                seen.add(text)

        if len(evidence) >= 50:
            break

    logger.debug(
        "Gathered %d evidence texts from %d/%d matched records for keyword %s",
        len(evidence),
        matched_records,
        scanned_records,
        keyword.keyword_id,
    )
    return evidence[:50]
