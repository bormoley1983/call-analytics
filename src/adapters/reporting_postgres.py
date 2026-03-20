from __future__ import annotations

from typing import Iterable

import psycopg2

from adapters.storage_postgres import _ensure_utf8_client_encoding
from domain.reporting import ReportCallRecord, ReportFilters


class PostgresReportingSource:
    source_name = "postgres"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _getconn(self):
        if self._conn is None or self._conn.closed:
            self._conn = _ensure_utf8_client_encoding(psycopg2.connect(self.dsn))
        return self._conn

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]:
        conn = self._getconn()
        clauses = ["1=1"]
        params: list[object] = []

        if filters.call_date_from:
            clauses.append("call_date >= %s")
            params.append(filters.call_date_from)
        if filters.call_date_to:
            clauses.append("call_date <= %s")
            params.append(filters.call_date_to)
        if filters.manager_id:
            clauses.append("manager_id = %s")
            params.append(filters.manager_id)
        if filters.role:
            clauses.append("role = %s")
            params.append(filters.role)
        if filters.direction:
            clauses.append("direction = %s")
            params.append(filters.direction)
        if filters.intent:
            clauses.append("intent = %s")
            params.append(filters.intent)
        if filters.outcome:
            clauses.append("outcome = %s")
            params.append(filters.outcome)

        query = f"""
            SELECT
                call_id,
                manager_id,
                manager_name,
                role,
                direction,
                spam_probability,
                effective_call,
                intent,
                outcome,
                summary,
                audio_seconds,
                call_date,
                src_number,
                dst_number,
                key_questions,
                objections
            FROM analyses
            WHERE {" AND ".join(clauses)}
            ORDER BY call_date NULLS LAST, call_id
        """

        with conn.cursor() as cur:
            cur.execute(query, params)
            for row in cur.fetchall():
                yield ReportCallRecord(
                    call_id=row[0],
                    manager_id=row[1] or "manager_unknown",
                    manager_name=row[2] or "Unknown/General",
                    role=row[3] or "unknown",
                    direction=row[4] or "unknown",
                    spam_probability=float(row[5] or 0.0),
                    effective_call=bool(row[6]),
                    intent=row[7] or "інше",
                    outcome=row[8] or "невідомо",
                    summary=row[9] or "",
                    audio_seconds=float(row[10] or 0.0),
                    call_date=row[11] or "",
                    src_number=row[12] or "",
                    dst_number=row[13] or "",
                    key_questions=list(row[14] or []),
                    objections=list(row[15] or []),
                )

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None
