from __future__ import annotations

import json
from typing import Any, Dict, Optional

import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS transcripts (
    call_id     TEXT PRIMARY KEY,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analyses (
    call_id         TEXT PRIMARY KEY,
    direction       TEXT,
    manager_id      TEXT,
    manager_name    TEXT,
    role            TEXT,
    spam_probability FLOAT,
    effective_call  BOOLEAN,
    intent          TEXT,
    outcome         TEXT,
    summary         TEXT,
    audio_seconds   FLOAT,
    call_date       TEXT,
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);
"""


class PostgresStorage:
    """Secondary storage layer — syncs processed call data to Postgres for reporting."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        with self._conn.cursor() as cur:
            cur.execute(DDL)
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def upsert_transcript(self, call_id: str, data: Dict[str, Any]) -> None:
        assert self._conn
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO transcripts (call_id, data)
                   VALUES (%s, %s)
                   ON CONFLICT (call_id) DO UPDATE SET data = EXCLUDED.data""",
                (call_id, json.dumps(data, ensure_ascii=False)),
            )
        self._conn.commit()

    def upsert_analysis(self, call_id: str, data: Dict[str, Any]) -> None:
        assert self._conn
        cm = data.get("call_meta", {})
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO analyses
                     (call_id, direction, manager_id, manager_name, role,
                      spam_probability, effective_call, intent, outcome,
                      summary, audio_seconds, call_date, data)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (call_id) DO UPDATE SET
                     data = EXCLUDED.data,
                     spam_probability = EXCLUDED.spam_probability,
                     effective_call = EXCLUDED.effective_call,
                     intent = EXCLUDED.intent,
                     outcome = EXCLUDED.outcome""",
                (
                    call_id,
                    cm.get("direction"),
                    data.get("manager_id"),
                    data.get("manager_name"),
                    data.get("role"),
                    data.get("spam_probability", 0.0),
                    bool(data.get("effective_call")),
                    data.get("intent"),
                    data.get("outcome"),
                    data.get("summary", ""),
                    cm.get("audio_seconds"),
                    cm.get("date"),
                    json.dumps(data, ensure_ascii=False),
                ),
            )
        self._conn.commit()

    def sync_per_call(self, per_call: list) -> None:
        """Bulk-sync a pipeline's per_call results into Postgres."""
        for item in per_call:
            if item.get("status") != "processed":
                continue
            meta = item.get("meta", {})
            call_id = meta.get("call_id")
            if not call_id:
                continue
            analysis = item.get("analysis", {})
            self.upsert_analysis(call_id, analysis)