from __future__ import annotations

import uuid
from typing import Any

import psycopg2

from adapters.storage_postgres import DDL, _ensure_utf8_client_encoding, _jsonb


class PostgresKeywordAiAnalysisStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _getconn(self):
        if self._conn is None or self._conn.closed:
            self._conn = _ensure_utf8_client_encoding(psycopg2.connect(self.dsn))
            with self._conn.cursor() as cur:
                cur.execute(DDL)
            self._conn.commit()
        return self._conn

    def save_analysis(
        self,
        *,
        request_data: dict[str, Any],
        analysis_input: dict[str, Any],
        ai_analysis: dict[str, Any],
        keyword_source: str,
        reporting_source: str | None,
        ai_model: str | None,
    ) -> dict[str, Any]:
        conn = self._getconn()
        analysis_id = str(uuid.uuid4())
        items = self._build_items(analysis_id=analysis_id, analysis_input=analysis_input, ai_analysis=ai_analysis)

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO keyword_ai_analyses (
                        analysis_id,
                        keyword_source,
                        reporting_source,
                        ai_model,
                        ai_summary,
                        analyzed_keywords,
                        total_candidates_before_limit,
                        truncated,
                        request_data,
                        analysis_input,
                        ai_analysis
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING created_at
                    """,
                    (
                        analysis_id,
                        keyword_source,
                        reporting_source,
                        ai_model,
                        str(ai_analysis.get("summary", "")).strip(),
                        int(analysis_input.get("analyzed_keywords", 0)),
                        int(analysis_input.get("total_candidates_before_limit", 0)),
                        bool(analysis_input.get("truncated", False)),
                        _jsonb(request_data),
                        _jsonb(analysis_input),
                        _jsonb(ai_analysis),
                    ),
                )
                created_at = cur.fetchone()[0]

                for item in items:
                    cur.execute(
                        """
                        INSERT INTO keyword_ai_analysis_items (analysis_id, item_type, item_key, data)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            item["analysis_id"],
                            item["item_type"],
                            item["item_key"],
                            _jsonb(item["data"]),
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return {
            "analysis_id": analysis_id,
            "created_at": created_at.isoformat(),
            "stored_items": len(items),
        }

    def list_analyses(self, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    analysis_id,
                    keyword_source,
                    reporting_source,
                    ai_model,
                    ai_summary,
                    analyzed_keywords,
                    total_candidates_before_limit,
                    truncated,
                    created_at
                FROM keyword_ai_analyses
                ORDER BY created_at DESC, analysis_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

        return [
            {
                "analysis_id": row[0],
                "keyword_source": row[1],
                "reporting_source": row[2],
                "ai_model": row[3],
                "ai_summary": row[4],
                "analyzed_keywords": int(row[5] or 0),
                "total_candidates_before_limit": int(row[6] or 0),
                "truncated": bool(row[7]),
                "created_at": row[8].isoformat(),
            }
            for row in rows
        ]

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        conn = self._getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    analysis_id,
                    keyword_source,
                    reporting_source,
                    ai_model,
                    ai_summary,
                    analyzed_keywords,
                    total_candidates_before_limit,
                    truncated,
                    request_data,
                    analysis_input,
                    ai_analysis,
                    created_at
                FROM keyword_ai_analyses
                WHERE analysis_id = %s
                """,
                (analysis_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None

            cur.execute(
                """
                SELECT item_type, item_key, data, created_at
                FROM keyword_ai_analysis_items
                WHERE analysis_id = %s
                ORDER BY item_type, item_key
                """,
                (analysis_id,),
            )
            item_rows = cur.fetchall()

        items_by_type: dict[str, list[dict[str, Any]]] = {}
        for item_type, item_key, data, created_at in item_rows:
            items_by_type.setdefault(item_type, []).append(
                {
                    "item_key": item_key,
                    "data": data or {},
                    "created_at": created_at.isoformat(),
                }
            )

        return {
            "analysis_id": row[0],
            "keyword_source": row[1],
            "reporting_source": row[2],
            "ai_model": row[3],
            "ai_summary": row[4],
            "analyzed_keywords": int(row[5] or 0),
            "total_candidates_before_limit": int(row[6] or 0),
            "truncated": bool(row[7]),
            "request": row[8] or {},
            "analysis_input": row[9] or {},
            "ai_analysis": row[10] or {},
            "created_at": row[11].isoformat(),
            "items": items_by_type,
        }

    def get_latest_analysis(self) -> dict[str, Any] | None:
        conn = self._getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT analysis_id
                FROM keyword_ai_analyses
                ORDER BY created_at DESC, analysis_id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if row is None:
            return None
        return self.get_analysis(str(row[0]))

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def _build_items(
        self,
        *,
        analysis_id: str,
        analysis_input: dict[str, Any],
        ai_analysis: dict[str, Any],
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []

        for keyword in analysis_input.get("keywords", []):
            keyword_id = str(keyword.get("keyword_id", "")).strip()
            if not keyword_id:
                continue
            items.append(
                {
                    "analysis_id": analysis_id,
                    "item_type": "keyword",
                    "item_key": keyword_id,
                    "data": keyword,
                }
            )

        for index, group in enumerate(ai_analysis.get("groups", [])):
            items.append(
                {
                    "analysis_id": analysis_id,
                    "item_type": "group",
                    "item_key": str(index),
                    "data": group,
                }
            )
            for action_index, action in enumerate(group.get("suggested_actions", [])):
                items.append(
                    {
                        "analysis_id": analysis_id,
                        "item_type": "action",
                        "item_key": f"{index}:{action_index}",
                        "data": {
                            "group_label": group.get("group_label"),
                            "group_theme": group.get("theme"),
                            **action,
                        },
                    }
                )

        for index, keyword_id in enumerate(ai_analysis.get("ungrouped_keyword_ids", [])):
            items.append(
                {
                    "analysis_id": analysis_id,
                    "item_type": "ungrouped",
                    "item_key": str(index),
                    "data": {"keyword_id": keyword_id},
                }
            )

        for index, recommendation in enumerate(ai_analysis.get("global_recommendations", [])):
            items.append(
                {
                    "analysis_id": analysis_id,
                    "item_type": "recommendation",
                    "item_key": str(index),
                    "data": {"text": recommendation},
                }
            )

        return items
