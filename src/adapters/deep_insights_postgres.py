from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List

from adapters.postgres_single_connection import SingleConnectionPostgresAdapter
from adapters.storage_postgres import DDL, _jsonb

logger = logging.getLogger(__name__)


class PostgresDeepInsightsStore(SingleConnectionPostgresAdapter):
    """Store for deep AI insights runs and individual insights."""

    def _initialize_connection(self, conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    def create_run(
        self,
        run_id: str,
        *,
        ai_model: str | None = None,
        insight_types: List[str] | None = None,
        request_data: Dict[str, Any] | None = None,
    ) -> str:
        """Create a new insights run record. Returns run_id."""

        def _write(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_deep_insights_runs
                        (run_id, ai_model, insight_types, request_data)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        ai_model,
                        _jsonb(insight_types or []),
                        _jsonb(request_data or {}),
                    ),
                )
            return run_id

        return self._run_write(_write)

    def add_insights(self, run_id: str, insights: List[Dict[str, Any]]) -> int:
        """Add insights to a run. Returns number of insights added."""

        def _write(conn):
            count = 0
            with conn.cursor() as cur:
                for ins in insights:
                    insight_id = str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO ai_deep_insights
                            (insight_id, run_id, insight_type, title, description,
                             severity, affected_calls_count, evidence_summary, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            insight_id,
                            run_id,
                            ins.get("insight_type", ""),
                            ins.get("title", ""),
                            ins.get("description", ""),
                            ins.get("severity", "low"),
                            ins.get("affected_calls_count", 0),
                            ins.get("evidence_summary"),
                            _jsonb(
                                {
                                    k: v
                                    for k, v in ins.items()
                                    if k
                                    not in (
                                        "insight_type",
                                        "title",
                                        "description",
                                        "severity",
                                        "affected_calls_count",
                                        "evidence_summary",
                                    )
                                }
                            ),
                        ),
                    )
                    count += 1
            return count

        return self._run_write(_write)

    def get_run(self, run_id: str) -> Dict[str, Any] | None:
        """Get a specific run with all its insights."""

        def _read(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, ai_model, insight_types, request_data, created_at
                    FROM ai_deep_insights_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                run_row = cur.fetchone()
                if not run_row:
                    return None

                cur.execute(
                    """
                    SELECT insight_id, insight_type, title, description, severity,
                           affected_calls_count, evidence_summary, metadata, created_at
                    FROM ai_deep_insights
                    WHERE run_id = %s
                    ORDER BY severity DESC, created_at
                    """,
                    (run_id,),
                )
                insight_rows = cur.fetchall()

            return {
                "run": self._run_row_to_dict(run_row),
                "insights": [self._insight_row_to_dict(row) for row in insight_rows],
            }

        return self._run_read(_read)

    def list_runs(
        self,
        limit: int = 50,
        insight_type_filter: str | None = None,
    ) -> List[Dict[str, Any]]:
        """List runs, optionally filtered by insight type."""

        def _read(conn):
            with conn.cursor() as cur:
                if insight_type_filter:
                    cur.execute(
                        """
                        SELECT r.run_id, r.ai_model, r.insight_types, r.created_at,
                               COUNT(i.insight_id) AS total_insights
                        FROM ai_deep_insights_runs r
                        LEFT JOIN ai_deep_insights i ON r.run_id = i.run_id
                            AND i.insight_type = %s
                        GROUP BY r.run_id, r.ai_model, r.insight_types, r.created_at
                        ORDER BY r.created_at DESC
                        LIMIT %s
                        """,
                        (insight_type_filter, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT r.run_id, r.ai_model, r.insight_types, r.created_at,
                               COUNT(i.insight_id) AS total_insights
                        FROM ai_deep_insights_runs r
                        LEFT JOIN ai_deep_insights i ON r.run_id = i.run_id
                        GROUP BY r.run_id, r.ai_model, r.insight_types, r.created_at
                        ORDER BY r.created_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                return cur.fetchall()

        rows = self._run_read(_read)
        return [self._list_run_row_to_dict(row) for row in rows]

    @staticmethod
    def _run_row_to_dict(row: tuple) -> Dict[str, Any]:
        return {
            "run_id": row[0],
            "ai_model": row[1],
            "insight_types": row[2] if isinstance(row[2], list) else [],
            "request_data": row[3],
            "created_at": (
                row[4].isoformat() if hasattr(row[4], "isoformat") else str(row[4])
            ),
        }

    @staticmethod
    def _insight_row_to_dict(row: tuple) -> Dict[str, Any]:
        return {
            "insight_id": row[0],
            "insight_type": row[1],
            "title": row[2],
            "description": row[3],
            "severity": row[4],
            "affected_calls_count": row[5],
            "evidence_summary": row[6],
            "metadata": row[7],
            "created_at": (
                row[8].isoformat() if hasattr(row[8], "isoformat") else str(row[8])
            ),
        }

    @staticmethod
    def _list_run_row_to_dict(row: tuple) -> Dict[str, Any]:
        insight_types = row[2] if isinstance(row[2], list) else []
        return {
            "run_id": row[0],
            "ai_model": row[1],
            "insight_types": insight_types,
            "total_insights": row[4],
            "created_at": (
                row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3])
            ),
        }
