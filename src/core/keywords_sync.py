from __future__ import annotations

from typing import Any

from ports.keywords import KeywordCatalogStore, KeywordSource


def sync_keywords_to_postgres(
    yaml_source: KeywordSource,
    postgres_source: KeywordCatalogStore,
    prune_missing: bool = False,
) -> dict[str, Any]:
    yaml_keywords = list(yaml_source.list_keywords())
    existing = {keyword.keyword_id: keyword for keyword in postgres_source.list_keywords()}

    synced_ids: list[str] = []
    for keyword in yaml_keywords:
        postgres_source.upsert_keyword(keyword)
        synced_ids.append(keyword.keyword_id)

    deleted_ids: list[str] = []
    if prune_missing:
        yaml_ids = {keyword.keyword_id for keyword in yaml_keywords}
        for keyword_id in sorted(existing):
            if keyword_id not in yaml_ids and postgres_source.delete_keyword(keyword_id):
                deleted_ids.append(keyword_id)

    return {
        "synced": len(synced_ids),
        "deleted": len(deleted_ids),
        "prune_missing": prune_missing,
        "synced_keyword_ids": sorted(synced_ids),
        "deleted_keyword_ids": deleted_ids,
    }
