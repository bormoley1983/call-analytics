from __future__ import annotations

from typing import Any

from core.keywords_materialize import materialize_call_keywords
from core.keywords_sync import sync_keywords_to_postgres
from ports.keywords import KeywordSource, RefreshableKeywordStore
from ports.reporting import ReportingSource


def refresh_keywords_data(
    *,
    yaml_source: KeywordSource,
    postgres_source: RefreshableKeywordStore,
    reporting_source: ReportingSource,
    prune_missing: bool = False,
) -> dict[str, Any]:
    sync_result = sync_keywords_to_postgres(
        yaml_source=yaml_source,
        postgres_source=postgres_source,
        prune_missing=prune_missing,
    )
    materialize_result = materialize_call_keywords(
        reporting_source=reporting_source,
        keyword_source=postgres_source,
        keyword_store=postgres_source,
        state_store=postgres_source,
    )
    return {
        "sync": sync_result,
        "materialize": materialize_result,
    }
