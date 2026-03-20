from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import psycopg2

from adapters.storage_postgres import DDL, _ensure_utf8_client_encoding, _jsonb
from domain.keywords import DEFAULT_MATCH_FIELDS, KeywordDefinition
from domain.reporting import ReportFilters


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PostgresKeywordSource:
    source_name = "postgres"

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

    def list_keywords(self) -> Iterable[KeywordDefinition]:
        conn = self._getconn()
        query = """
            SELECT
                k.keyword_id,
                k.label,
                k.category,
                k.match_fields,
                k.is_active,
                COALESCE(
                    ARRAY(
                        SELECT ka.phrase
                        FROM keyword_aliases ka
                        WHERE ka.keyword_id = k.keyword_id
                        ORDER BY ka.phrase
                    ),
                    ARRAY[]::TEXT[]
                ) AS terms
            FROM keywords k
            ORDER BY k.category, k.label, k.keyword_id
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        return [
            KeywordDefinition(
                keyword_id=row[0],
                label=row[1],
                category=row[2] or "general",
                match_fields=list(row[3] or DEFAULT_MATCH_FIELDS),
                is_active=bool(row[4]),
                terms=list(row[5] or []),
            )
            for row in rows
        ]

    def get_keyword(self, keyword_id: str) -> KeywordDefinition | None:
        conn = self._getconn()
        query = """
            SELECT
                k.keyword_id,
                k.label,
                k.category,
                k.match_fields,
                k.is_active,
                COALESCE(
                    ARRAY(
                        SELECT ka.phrase
                        FROM keyword_aliases ka
                        WHERE ka.keyword_id = k.keyword_id
                        ORDER BY ka.phrase
                    ),
                    ARRAY[]::TEXT[]
                ) AS terms
            FROM keywords k
            WHERE k.keyword_id = %s
        """
        with conn.cursor() as cur:
            cur.execute(query, (keyword_id,))
            row = cur.fetchone()
        if row is None:
            return None
        return KeywordDefinition(
            keyword_id=row[0],
            label=row[1],
            category=row[2] or "general",
            match_fields=list(row[3] or DEFAULT_MATCH_FIELDS),
            is_active=bool(row[4]),
            terms=list(row[5] or []),
        )

    def upsert_keyword(self, keyword: KeywordDefinition) -> KeywordDefinition:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO keywords (keyword_id, label, category, match_fields, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (keyword_id) DO UPDATE SET
                        label = EXCLUDED.label,
                        category = EXCLUDED.category,
                        match_fields = EXCLUDED.match_fields,
                        is_active = EXCLUDED.is_active
                    """,
                    (
                        keyword.keyword_id,
                        keyword.label,
                        keyword.category,
                        keyword.match_fields or list(DEFAULT_MATCH_FIELDS),
                        keyword.is_active,
                    ),
                )
                cur.execute("DELETE FROM keyword_aliases WHERE keyword_id = %s", (keyword.keyword_id,))
                for term in keyword.terms:
                    cur.execute(
                        """
                        INSERT INTO keyword_aliases (keyword_id, phrase)
                        VALUES (%s, %s)
                        ON CONFLICT (keyword_id, phrase) DO NOTHING
                        """,
                        (keyword.keyword_id, term),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        created = self.get_keyword(keyword.keyword_id)
        assert created is not None
        return created

    def delete_keyword(self, keyword_id: str) -> bool:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM keywords WHERE keyword_id = %s", (keyword_id,))
                deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            raise

    def replace_call_keyword_matches(self, call_id: str, rows: list[dict]) -> None:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM call_keywords WHERE call_id = %s", (call_id,))
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO call_keywords
                            (call_id, keyword_id, match_count, matched_fields, matched_terms, updated_at)
                        VALUES (%s, %s, %s, %s, %s, now())
                        """,
                        (
                            call_id,
                            row["keyword_id"],
                            row["match_count"],
                            _jsonb(row.get("matched_fields") or []),
                            _jsonb(row.get("matched_terms") or []),
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def is_materialized(self) -> bool:
        conn = self._getconn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM keyword_materialization_state WHERE state_key = %s",
                ("default",),
            )
            return cur.fetchone() is not None

    def mark_materialization_completed(self, processed_calls: int, matched_calls: int, stored_rows: int) -> None:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO keyword_materialization_state
                        (state_key, last_materialized_at, processed_calls, matched_calls, stored_rows)
                    VALUES (%s, now(), %s, %s, %s)
                    ON CONFLICT (state_key) DO UPDATE SET
                        last_materialized_at = EXCLUDED.last_materialized_at,
                        processed_calls = EXCLUDED.processed_calls,
                        matched_calls = EXCLUDED.matched_calls,
                        stored_rows = EXCLUDED.stored_rows
                    """,
                    ("default", processed_calls, matched_calls, stored_rows),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def build_materialized_keywords_report(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "matched_calls",
        order: str = "desc",
    ) -> dict:
        conn = self._getconn()
        clauses = ["1=1"]
        params: list[object] = []

        if filters.call_date_from:
            clauses.append("a.call_date >= %s")
            params.append(filters.call_date_from)
        if filters.call_date_to:
            clauses.append("a.call_date <= %s")
            params.append(filters.call_date_to)
        if filters.manager_id:
            clauses.append("a.manager_id = %s")
            params.append(filters.manager_id)
        if filters.role:
            clauses.append("a.role = %s")
            params.append(filters.role)
        if filters.direction:
            clauses.append("a.direction = %s")
            params.append(filters.direction)
        if filters.intent:
            clauses.append("a.intent = %s")
            params.append(filters.intent)
        if filters.outcome:
            clauses.append("a.outcome = %s")
            params.append(filters.outcome)
        if filters.spam_only:
            clauses.append("COALESCE(a.spam_probability, 0) >= %s")
            params.append(spam_threshold)
        if filters.effective_only:
            clauses.append("COALESCE(a.effective_call, FALSE) = TRUE")

        query = f"""
            SELECT
                k.keyword_id,
                k.label,
                k.category,
                k.match_fields,
                COALESCE(
                    ARRAY(
                        SELECT ka.phrase
                        FROM keyword_aliases ka
                        WHERE ka.keyword_id = k.keyword_id
                        ORDER BY ka.phrase
                    ),
                    ARRAY[]::TEXT[]
                ) AS terms,
                ck.call_id,
                ck.match_count,
                a.manager_id,
                a.intent,
                a.outcome
            FROM call_keywords ck
            JOIN keywords k ON k.keyword_id = ck.keyword_id
            JOIN analyses a ON a.call_id = ck.call_id
            WHERE {" AND ".join(clauses)}
            ORDER BY k.category, k.label, k.keyword_id, ck.call_id
        """

        buckets: dict[str, dict] = {}
        with conn.cursor() as cur:
            cur.execute(query, params)
            for row in cur.fetchall():
                bucket = buckets.setdefault(
                    row[0],
                    {
                        "keyword_id": row[0],
                        "label": row[1],
                        "category": row[2] or "general",
                        "match_fields": list(row[3] or DEFAULT_MATCH_FIELDS),
                        "terms": list(row[4] or []),
                        "matched_calls": 0,
                        "total_matches": 0,
                        "matched_managers": set(),
                        "intents": {},
                        "outcomes": {},
                    },
                )
                bucket["matched_calls"] += 1
                bucket["total_matches"] += int(row[6] or 0)
                bucket["matched_managers"].add(row[7] or "manager_unknown")
                bucket["intents"][row[8] or "інше"] = bucket["intents"].get(row[8] or "інше", 0) + 1
                bucket["outcomes"][row[9] or "невідомо"] = bucket["outcomes"].get(row[9] or "невідомо", 0) + 1

        all_keywords = []
        for keyword in [keyword for keyword in self.list_keywords() if keyword.is_active and keyword.terms]:
            bucket = buckets.get(
                keyword.keyword_id,
                {
                    "keyword_id": keyword.keyword_id,
                    "label": keyword.label,
                    "category": keyword.category,
                    "match_fields": keyword.match_fields,
                    "terms": keyword.terms,
                    "matched_calls": 0,
                    "total_matches": 0,
                    "matched_managers": set(),
                    "intents": {},
                    "outcomes": {},
                },
            )
            all_keywords.append(
                {
                    "keyword_id": bucket["keyword_id"],
                    "label": bucket["label"],
                    "category": bucket["category"],
                    "terms": bucket["terms"],
                    "match_fields": bucket["match_fields"],
                    "matched_calls": bucket["matched_calls"],
                    "total_matches": bucket["total_matches"],
                    "matched_managers": len(bucket["matched_managers"]),
                    "top_intents": sorted(bucket["intents"].items(), key=lambda kv: kv[1], reverse=True)[:10],
                    "top_outcomes": sorted(bucket["outcomes"].items(), key=lambda kv: kv[1], reverse=True)[:5],
                }
            )

        reverse = order == "desc"
        if sort_by in {"label", "category"}:
            all_keywords.sort(
                key=lambda item: (item[sort_by], item["label"], item["keyword_id"]),
                reverse=reverse,
            )
        else:
            all_keywords.sort(
                key=lambda item: (item.get(sort_by, 0), item["category"], item["label"], item["keyword_id"]),
                reverse=reverse,
            )

        return {
            "generated_at": _utc_now_iso(),
            "report_data_source": "postgres_materialized",
            "keyword_data_source": self.source_name,
            "filters": filters.as_dict(),
            "total_keywords": len(all_keywords),
            "keywords_with_matches": sum(1 for item in all_keywords if item["matched_calls"] > 0),
            "keywords": all_keywords,
        }

    def _analysis_filter_clauses(self, filters: ReportFilters, spam_threshold: float) -> tuple[list[str], list[object]]:
        clauses = ["1=1"]
        params: list[object] = []

        if filters.call_date_from:
            clauses.append("a.call_date >= %s")
            params.append(filters.call_date_from)
        if filters.call_date_to:
            clauses.append("a.call_date <= %s")
            params.append(filters.call_date_to)
        if filters.manager_id:
            clauses.append("a.manager_id = %s")
            params.append(filters.manager_id)
        if filters.role:
            clauses.append("a.role = %s")
            params.append(filters.role)
        if filters.direction:
            clauses.append("a.direction = %s")
            params.append(filters.direction)
        if filters.intent:
            clauses.append("a.intent = %s")
            params.append(filters.intent)
        if filters.outcome:
            clauses.append("a.outcome = %s")
            params.append(filters.outcome)
        if filters.spam_only:
            clauses.append("COALESCE(a.spam_probability, 0) >= %s")
            params.append(spam_threshold)
        if filters.effective_only:
            clauses.append("COALESCE(a.effective_call, FALSE) = TRUE")
        return clauses, params

    def build_keyword_calls_report(
        self,
        keyword_id: str,
        filters: ReportFilters,
        spam_threshold: float,
        limit: int,
        offset: int,
        sort_by: str = "call_date",
        order: str = "desc",
    ) -> dict:
        conn = self._getconn()
        clauses, params = self._analysis_filter_clauses(filters, spam_threshold)
        params = [keyword_id, *params]
        count_params = list(params)
        params.extend([limit, offset])

        order_by_map = {
            "call_date": "a.call_date",
            "match_count": "ck.match_count",
            "manager_name": "a.manager_name",
            "intent": "a.intent",
            "outcome": "a.outcome",
        }
        order_by = order_by_map.get(sort_by, "a.call_date")
        order_dir = "ASC" if order == "asc" else "DESC"
        count_query = f"""
            SELECT COUNT(*)
            FROM call_keywords ck
            JOIN analyses a ON a.call_id = ck.call_id
            WHERE ck.keyword_id = %s AND {" AND ".join(clauses)}
        """
        data_query = f"""
            SELECT
                ck.call_id,
                ck.match_count,
                ck.matched_fields,
                ck.matched_terms,
                a.call_date,
                a.manager_id,
                a.manager_name,
                a.role,
                a.direction,
                a.intent,
                a.outcome,
                a.summary,
                a.audio_seconds,
                a.spam_probability,
                a.effective_call
            FROM call_keywords ck
            JOIN analyses a ON a.call_id = ck.call_id
            WHERE ck.keyword_id = %s AND {" AND ".join(clauses)}
            ORDER BY {order_by} {order_dir} NULLS LAST, ck.call_id DESC
            LIMIT %s OFFSET %s
        """
        with conn.cursor() as cur:
            cur.execute(count_query, count_params)
            total = int(cur.fetchone()[0])
            cur.execute(data_query, params)
            rows = cur.fetchall()

        calls = [
            {
                "call_id": row[0],
                "match_count": int(row[1] or 0),
                "matched_fields": list(row[2] or []),
                "matched_terms": list(row[3] or []),
                "call_date": row[4] or "",
                "manager_id": row[5] or "manager_unknown",
                "manager_name": row[6] or "Unknown/General",
                "role": row[7] or "unknown",
                "direction": row[8] or "unknown",
                "intent": row[9] or "інше",
                "outcome": row[10] or "невідомо",
                "summary": row[11] or "",
                "audio_seconds": float(row[12] or 0.0),
                "spam_probability": float(row[13] or 0.0),
                "effective_call": bool(row[14]),
            }
            for row in rows
        ]
        return {
            "generated_at": _utc_now_iso(),
            "report_data_source": "postgres_materialized",
            "filters": filters.as_dict(),
            "keyword_id": keyword_id,
            "total_calls": total,
            "limit": limit,
            "offset": offset,
            "calls": calls,
        }

    def build_keyword_trend_report(self, keyword_id: str, filters: ReportFilters, spam_threshold: float) -> dict:
        conn = self._getconn()
        clauses, params = self._analysis_filter_clauses(filters, spam_threshold)
        params = [keyword_id, *params]
        query = f"""
            SELECT
                a.call_date,
                COUNT(*) AS matched_calls,
                COALESCE(SUM(ck.match_count), 0) AS total_matches
            FROM call_keywords ck
            JOIN analyses a ON a.call_id = ck.call_id
            WHERE ck.keyword_id = %s AND {" AND ".join(clauses)}
            GROUP BY a.call_date
            ORDER BY a.call_date
        """
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return {
            "generated_at": _utc_now_iso(),
            "report_data_source": "postgres_materialized",
            "filters": filters.as_dict(),
            "keyword_id": keyword_id,
            "points": [
                {
                    "call_date": row[0] or "",
                    "matched_calls": int(row[1] or 0),
                    "total_matches": int(row[2] or 0),
                }
                for row in rows
            ],
        }

    def build_keyword_managers_report(
        self,
        keyword_id: str,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "matched_calls",
        order: str = "desc",
    ) -> dict:
        conn = self._getconn()
        clauses, params = self._analysis_filter_clauses(filters, spam_threshold)
        params = [keyword_id, *params]
        order_by_map = {
            "matched_calls": "matched_calls",
            "total_matches": "total_matches",
            "manager_name": "a.manager_name",
        }
        order_by = order_by_map.get(sort_by, "matched_calls")
        order_dir = "ASC" if order == "asc" else "DESC"
        query = f"""
            SELECT
                a.manager_id,
                a.manager_name,
                a.role,
                COUNT(*) AS matched_calls,
                COALESCE(SUM(ck.match_count), 0) AS total_matches
            FROM call_keywords ck
            JOIN analyses a ON a.call_id = ck.call_id
            WHERE ck.keyword_id = %s AND {" AND ".join(clauses)}
            GROUP BY a.manager_id, a.manager_name, a.role
            ORDER BY {order_by} {order_dir}, total_matches DESC, a.manager_name, a.manager_id
        """
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return {
            "generated_at": _utc_now_iso(),
            "report_data_source": "postgres_materialized",
            "filters": filters.as_dict(),
            "keyword_id": keyword_id,
            "managers": [
                {
                    "manager_id": row[0] or "manager_unknown",
                    "manager_name": row[1] or "Unknown/General",
                    "role": row[2] or "unknown",
                    "matched_calls": int(row[3] or 0),
                    "total_matches": int(row[4] or 0),
                }
                for row in rows
            ],
        }

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None
