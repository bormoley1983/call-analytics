# -*- coding: utf-8 -*-
"""PostgreSQL adapter for AI apply operations.

Persists apply records to the ai_apply table and queries apply history.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from adapters.postgres_single_connection import SingleConnectionPostgresAdapter

logger = logging.getLogger(__name__)


class PostgresAiApplyStore(SingleConnectionPostgresAdapter):
    """PostgreSQL adapter for persisting and querying AI apply operations."""

    TABLE_NAME = "ai_apply"

    def apply_actions(
        self,
        analysis_id: str,
        applied_by: Optional[str],
        dry_run: bool,
        actions_applied: List[Dict[str, Any]],
        actions_skipped: List[Dict[str, Any]],
        mutations: List[Dict[str, Any]],
        keyword_refreshed: bool = False,
        follow_up_ran: bool = False,
        error: Optional[str] = None,
    ) -> str:
        """Persist an apply record to the ai_apply table.

        Args:
            analysis_id: UUID of the source analysis.
            applied_by: User/operator who triggered the apply.
            dry_run: Whether this was a dry-run (no mutations applied).
            actions_applied: List of actions that were successfully applied.
            actions_skipped: List of actions that were skipped with reasons.
            mutations: List of mutations that were performed (or previewed).
            keyword_refreshed: Whether keyword materialization was triggered.
            follow_up_ran: Whether a follow-up analysis was triggered.
            error: Error message if the apply failed.

        Returns:
            apply_id: UUID of the created apply record.
        """
        apply_id = str(uuid.uuid4())
        applied_at = datetime.now(timezone.utc)

        def _apply(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_apply (
                        apply_id, analysis_id, applied_at, applied_by,
                        dry_run, actions_applied, actions_skipped, mutations,
                        keyword_refreshed, follow_up_ran, error
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING apply_id
                    """,
                    (
                        apply_id,
                        analysis_id,
                        applied_at.isoformat(),
                        applied_by,
                        dry_run,
                        json.dumps(actions_applied, ensure_ascii=False),
                        json.dumps(actions_skipped, ensure_ascii=False),
                        json.dumps(mutations, ensure_ascii=False),
                        keyword_refreshed,
                        follow_up_ran,
                        error,
                    ),
                )
                cur.fetchone()

        self._run_write(_apply)
        logger.info(
            "AI apply record saved: apply_id=%s analysis_id=%s dry_run=%s",
            apply_id,
            analysis_id,
            dry_run,
        )

        return apply_id

    def get_apply_history(
        self,
        analysis_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get apply history for a specific analysis.

        Args:
            analysis_id: UUID of the analysis to get history for.
            limit: Maximum number of records to return.
            offset: Offset for pagination.

        Returns:
            List of apply records, most recent first.
        """

        def _get_history(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        apply_id, analysis_id, applied_at, applied_by,
                        dry_run, actions_applied, actions_skipped, mutations,
                        keyword_refreshed, follow_up_ran, error
                    FROM ai_apply
                    WHERE analysis_id = %s
                    ORDER BY applied_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (analysis_id, limit, offset),
                )
                columns = [desc[0] for desc in cur.description]
                return columns, cur.fetchall()

        columns, rows = self._run_read(_get_history)

        results = []
        for row in rows:
            record = dict(zip(columns, row))
            # Parse JSONB fields
            for json_field in ("actions_applied", "actions_skipped", "mutations"):
                val = record.get(json_field)
                if isinstance(val, str):
                    record[json_field] = json.loads(val)
                elif val is None:
                    record[json_field] = []
            results.append(record)

        return results
