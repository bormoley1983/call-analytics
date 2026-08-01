from __future__ import annotations

import logging
from typing import Any, Dict, List

from ports.llm import LlmPort
from ports.reporting import ReportingSource

logger = logging.getLogger(__name__)


def expand_keyword_aliases(
    keyword_id: str,
    keyword_source: Any,  # needs get_keyword() — PostgresKeywordSource
    reporting_source: ReportingSource,
    llm: LlmPort,
    *,
    max_aliases: int = 5,
    filters: Any | None = None,
) -> Dict[str, Any]:
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

    # Gather evidence: recent matched texts from analyses
    evidence_texts = _gather_evidence_texts(
        reporting_source, keyword_id, filters=filters
    )

    logger.info(
        "Expanding aliases for keyword %s: current_terms=%d evidence_texts=%d",
        keyword_id,
        len(current_terms),
        len(evidence_texts),
    )

    # Call LLM for alias suggestions
    llm_result: Dict[str, Any] = llm.expand_aliases(
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
    keyword_id: str,
    *,
    filters: Any | None = None,
) -> List[str]:
    """Gather recent matched texts from analyses for evidence.

    Scans analysis records and collects summary/key_questions/objections
    that match the keyword's terms.
    """
    from domain.reporting import ReportFilters

    effective_filters = filters or ReportFilters()
    evidence: List[str] = []
    seen: set[str] = set()

    # We need to get the keyword terms first to check for matches
    # This is a simplified approach — in practice, we'd query call_keywords
    # For now, collect texts from recent analyses as context
    record_count = 0
    for record in reporting_source.iter_call_records(effective_filters):
        if record.summary and record.summary not in seen:
            evidence.append(record.summary)
            seen.add(record.summary)
        for q in record.key_questions:
            if q and q not in seen:
                evidence.append(q)
                seen.add(q)
        for o in record.objections:
            if o and o not in seen:
                evidence.append(o)
                seen.add(o)

        record_count += 1
        if len(evidence) >= 50:
            break

    logger.debug(
        "Gathered %d evidence texts from %d records for keyword %s",
        len(evidence),
        record_count,
        keyword_id,
    )
    return evidence[:50]
