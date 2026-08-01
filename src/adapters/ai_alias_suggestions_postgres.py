from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List

from adapters.postgres_single_connection import SingleConnectionPostgresAdapter
from adapters.storage_postgres import DDL, _jsonb

logger = logging.getLogger(__name__)


class PostgresAiAliasSuggestionStore(SingleConnectionPostgresAdapter):
    """Store for AI-generated alias suggestions with provenance tracking."""

    def _initialize_connection(self, conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    def create_suggestion(
        self,
        keyword_id: str,
        suggested_aliases: List[Dict[str, Any]],
        *,
        source_evidence: Dict[str, Any] | None = None,
        ai_model: str | None = None,
    ) -> str:
        """Create a new alias suggestion record. Returns suggestion_id."""

        def _write(conn):
            suggestion_id = str(uuid.uuid4())
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_alias_suggestions
                        (suggestion_id, keyword_id, suggested_aliases, source_evidence, ai_model, status)
                    VALUES (%s, %s, %s, %s, %s, 'pending')
                    """,
                    (
                        suggestion_id,
                        keyword_id,
                        _jsonb(suggested_aliases),
                        _jsonb(source_evidence) if source_evidence else None,
                        ai_model,
                    ),
                )
            return suggestion_id

        return self._run_write(_write)

    def get_suggestion(self, suggestion_id: str) -> Dict[str, Any] | None:
        """Get a single suggestion by ID."""

        def _read(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT suggestion_id, keyword_id, suggested_aliases, source_evidence,
                           ai_model, status, created_at
                    FROM ai_alias_suggestions
                    WHERE suggestion_id = %s
                    """,
                    (suggestion_id,),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

        return self._run_read(_read)

    def list_suggestions(
        self, keyword_id: str | None = None, status: str | None = None
    ) -> List[Dict[str, Any]]:
        """List suggestions, optionally filtered by keyword_id and/or status."""
        conditions: List[str] = []
        params: List[Any] = []

        if keyword_id:
            conditions.append("keyword_id = %s")
            params.append(keyword_id)
        if status:
            conditions.append("status = %s")
            params.append(status)

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        def _read(conn):
            query = f"""
                SELECT suggestion_id, keyword_id, suggested_aliases, source_evidence,
                       ai_model, status, created_at
                FROM ai_alias_suggestions
                {where_clause}
                ORDER BY created_at DESC
            """
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

        rows = self._run_read(_read)
        return [self._row_to_dict(row) for row in rows]

    def approve_suggestion(self, suggestion_id: str) -> bool:
        """Approve a suggestion and move aliases into keyword_aliases table."""

        def _write(conn):
            with conn.cursor() as cur:
                # Get the suggestion
                cur.execute(
                    """
                    SELECT keyword_id, suggested_aliases, status
                    FROM ai_alias_suggestions
                    WHERE suggestion_id = %s
                    """,
                    (suggestion_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False

                kw_id = row[0]
                aliases_data = row[1]
                current_status = row[2]

                if current_status == "approved":
                    return True  # Already approved

                # Insert aliases into keyword_aliases table
                if isinstance(aliases_data, list):
                    for alias_item in aliases_data:
                        phrase = None
                        if isinstance(alias_item, dict):
                            phrase = alias_item.get("phrase")
                        elif isinstance(alias_item, str):
                            phrase = alias_item

                        if phrase:
                            cur.execute(
                                """
                                INSERT INTO keyword_aliases (keyword_id, phrase)
                                VALUES (%s, %s)
                                ON CONFLICT (keyword_id, phrase) DO NOTHING
                                """,
                                (kw_id, phrase),
                            )

                # Update status
                cur.execute(
                    "UPDATE ai_alias_suggestions SET status = 'approved' WHERE suggestion_id = %s",
                    (suggestion_id,),
                )

            return True

        return self._run_write(_write)

    def reject_suggestion(self, suggestion_id: str) -> bool:
        """Reject a suggestion."""

        def _write(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ai_alias_suggestions SET status = 'rejected' WHERE suggestion_id = %s",
                    (suggestion_id,),
                )
                return cur.rowcount > 0

        return self._run_write(_write)

    @staticmethod
    def _row_to_dict(row: tuple) -> Dict[str, Any]:
        return {
            "suggestion_id": row[0],
            "keyword_id": row[1],
            "suggested_aliases": row[2] if isinstance(row[2], list) else [],
            "source_evidence": row[3],
            "ai_model": row[4],
            "status": row[5],
            "created_at": (
                row[6].isoformat() if hasattr(row[6], "isoformat") else str(row[6])
            ),
        }
