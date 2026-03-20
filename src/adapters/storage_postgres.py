from __future__ import annotations

import json
from typing import Any, Dict, Optional

from psycopg2 import extensions as pg_extensions
from psycopg2 import pool as pg_pool
from psycopg2.extras import Json

DDL = """
CREATE TABLE IF NOT EXISTS transcripts (
    call_id     TEXT PRIMARY KEY,
    pipeline_stage TEXT,
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
    src_number       TEXT,
    dst_number       TEXT,
    key_questions    JSONB,
    objections       JSONB,
    analysis_error   TEXT,
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id    TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    category      TEXT NOT NULL DEFAULT 'general',
    match_fields  JSONB NOT NULL DEFAULT '["summary","key_questions","objections"]'::jsonb,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keyword_aliases (
    keyword_id    TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    phrase        TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (keyword_id, phrase)
);

CREATE TABLE IF NOT EXISTS call_keywords (
    call_id          TEXT NOT NULL REFERENCES analyses(call_id) ON DELETE CASCADE,
    keyword_id       TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    match_count      INTEGER NOT NULL DEFAULT 0,
    matched_fields   JSONB NOT NULL DEFAULT '[]'::jsonb,
    matched_terms    JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (call_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS keyword_materialization_state (
    state_key            TEXT PRIMARY KEY,
    last_materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_calls      INTEGER NOT NULL DEFAULT 0,
    matched_calls        INTEGER NOT NULL DEFAULT 0,
    stored_rows          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS keyword_ai_analyses (
    analysis_id                     TEXT PRIMARY KEY,
    keyword_source                  TEXT NOT NULL,
    reporting_source                TEXT,
    ai_model                        TEXT,
    ai_summary                      TEXT NOT NULL DEFAULT '',
    analyzed_keywords               INTEGER NOT NULL DEFAULT 0,
    total_candidates_before_limit   INTEGER NOT NULL DEFAULT 0,
    truncated                       BOOLEAN NOT NULL DEFAULT FALSE,
    request_data                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    analysis_input                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    ai_analysis                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keyword_ai_analysis_items (
    analysis_id  TEXT NOT NULL REFERENCES keyword_ai_analyses(analysis_id) ON DELETE CASCADE,
    item_type    TEXT NOT NULL,
    item_key     TEXT NOT NULL,
    data         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (analysis_id, item_type, item_key)
);

ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS pipeline_stage TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS direction TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS manager_id TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS manager_name TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS role TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS spam_probability FLOAT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS effective_call BOOLEAN;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS intent TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS outcome TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS summary TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS audio_seconds FLOAT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS call_date TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS src_number TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS dst_number TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS key_questions JSONB;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS objections JSONB;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS analysis_error TEXT;
ALTER TABLE keywords ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE keywords ADD COLUMN IF NOT EXISTS match_fields JSONB;
ALTER TABLE keywords ADD COLUMN IF NOT EXISTS is_active BOOLEAN;
ALTER TABLE call_keywords ADD COLUMN IF NOT EXISTS match_count INTEGER;
ALTER TABLE call_keywords ADD COLUMN IF NOT EXISTS matched_fields JSONB;
ALTER TABLE call_keywords ADD COLUMN IF NOT EXISTS matched_terms JSONB;
ALTER TABLE call_keywords ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE keyword_materialization_state ADD COLUMN IF NOT EXISTS last_materialized_at TIMESTAMPTZ;
ALTER TABLE keyword_materialization_state ADD COLUMN IF NOT EXISTS processed_calls INTEGER;
ALTER TABLE keyword_materialization_state ADD COLUMN IF NOT EXISTS matched_calls INTEGER;
ALTER TABLE keyword_materialization_state ADD COLUMN IF NOT EXISTS stored_rows INTEGER;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS keyword_source TEXT;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS reporting_source TEXT;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS ai_model TEXT;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS ai_summary TEXT;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS analyzed_keywords INTEGER;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS total_candidates_before_limit INTEGER;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS truncated BOOLEAN;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS request_data JSONB;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS analysis_input JSONB;
ALTER TABLE keyword_ai_analyses ADD COLUMN IF NOT EXISTS ai_analysis JSONB;
ALTER TABLE keyword_ai_analysis_items ADD COLUMN IF NOT EXISTS data JSONB;
ALTER TABLE keyword_ai_analysis_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_analyses_call_date ON analyses(call_date);
CREATE INDEX IF NOT EXISTS idx_analyses_manager_id ON analyses(manager_id);
CREATE INDEX IF NOT EXISTS idx_analyses_role ON analyses(role);
CREATE INDEX IF NOT EXISTS idx_analyses_intent ON analyses(intent);
CREATE INDEX IF NOT EXISTS idx_analyses_outcome ON analyses(outcome);
CREATE INDEX IF NOT EXISTS idx_analyses_direction ON analyses(direction);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_id ON call_keywords(keyword_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_call_id ON call_keywords(call_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_call ON call_keywords(keyword_id, call_id);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analyses_created_at ON keyword_ai_analyses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analysis_items_analysis_id ON keyword_ai_analysis_items(analysis_id);
"""


def _jsonb(value: Any) -> Json:
    return Json(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))


def _ensure_utf8_client_encoding(conn: Any) -> Any:
    if getattr(conn, "encoding", "").upper() != "UTF8":
        conn.set_client_encoding("UTF8")
    return conn


def _infer_transcript_stage(data: Dict[str, Any]) -> Optional[str]:
    stage = data.get("_pipeline_stage")
    if isinstance(stage, str) and stage:
        return stage
    if data.get("text_uk") or data.get("segments_uk"):
        return "translated"
    if data.get("text") or data.get("segments"):
        return "transcribed"
    return None


def _transcript_row(call_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "call_id": call_id,
        "pipeline_stage": _infer_transcript_stage(data),
        "data": _jsonb(data),
    }


def _analysis_row(call_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    call_meta = data.get("call_meta") or {}
    spam_probability = data.get("spam_probability", 0.0)
    try:
        spam_probability = float(spam_probability)
    except (TypeError, ValueError):
        spam_probability = 0.0

    effective_call = data.get("effective_call")
    if isinstance(effective_call, str):
        effective_call = effective_call.strip().lower() in {"1", "true", "yes", "tak", "так"}
    else:
        effective_call = bool(effective_call)

    return {
        "call_id": call_id,
        "direction": call_meta.get("direction"),
        "manager_id": data.get("manager_id"),
        "manager_name": data.get("manager_name"),
        "role": data.get("role"),
        "spam_probability": spam_probability,
        "effective_call": effective_call,
        "intent": data.get("intent"),
        "outcome": data.get("outcome"),
        "summary": data.get("summary", ""),
        "audio_seconds": call_meta.get("audio_seconds"),
        "call_date": call_meta.get("date"),
        "src_number": call_meta.get("src_number"),
        "dst_number": call_meta.get("dst_number"),
        "key_questions": _jsonb(data.get("key_questions") or []),
        "objections": _jsonb(data.get("objections") or []),
        "analysis_error": data.get("analysis_error"),
        "data": _jsonb(data),
    }


class PostgresStorage:
    """Secondary storage layer — syncs processed call data to Postgres for reporting.

    Uses a ThreadedConnectionPool so multiple analysis workers can safely
    read/write concurrently — a single psycopg2 connection is not thread-safe.
    """

    def __init__(self, dsn: str, max_connections: int = 10):
        self.dsn = dsn
        self.max_connections = max_connections
        self._pool: Optional[pg_pool.ThreadedConnectionPool] = None

    def _require_pool(self) -> pg_pool.ThreadedConnectionPool:
        if self._pool is None:
            raise RuntimeError("PostgresStorage is not initialized. Call ensure_ready() first.")
        return self._pool

    def _getconn(self) -> pg_extensions.connection:
        return _ensure_utf8_client_encoding(self._require_pool().getconn())

    def _putconn(self, conn: pg_extensions.connection) -> None:
        self._require_pool().putconn(conn)

    # --- lifecycle ---   
     
    def ensure_ready(self) -> None:
        self._pool = pg_pool.ThreadedConnectionPool(1, self.max_connections, self.dsn)
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.commit()
        finally:
            self._putconn(conn)

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            self._pool = None

    # --- StoragePort interface ---  
     
    def transcript_exists(self, call_id: str) -> bool:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM transcripts WHERE call_id = %s", (call_id,))
                return cur.fetchone() is not None
        finally:
            self._putconn(conn)

    def analysis_exists(self, call_id: str) -> bool:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM analyses WHERE call_id = %s", (call_id,))
                return cur.fetchone() is not None
        finally:
            self._putconn(conn)

    def load_transcript(self, call_id: str) -> Dict[str, Any]:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pipeline_stage, data FROM transcripts WHERE call_id = %s",
                    (call_id,),
                )
                row = cur.fetchone()
        finally:
            self._putconn(conn)
        if row is None:
            raise KeyError(f"Transcript not found: {call_id}")
        pipeline_stage, data = row
        if pipeline_stage and "_pipeline_stage" not in data:
            data["_pipeline_stage"] = pipeline_stage
        elif "_pipeline_stage" not in data:
            inferred = _infer_transcript_stage(data)
            if inferred:
                data["_pipeline_stage"] = inferred
        return data  # psycopg2 deserialises JSONB columns to dict automatically

    def load_analysis(self, call_id: str) -> Dict[str, Any]:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM analyses WHERE call_id = %s", (call_id,))
                row = cur.fetchone()
        finally:
            self._putconn(conn)
        if row is None:
            raise KeyError(f"Analysis not found: {call_id}")
        return row[0]

    def save_transcript(self, call_id: str, data: Dict[str, Any]) -> None:
        self.upsert_transcript(call_id, data)

    def save_analysis(self, call_id: str, data: Dict[str, Any]) -> None:
        self.upsert_analysis(call_id, data)          

    # --- upsert helpers (also kept for sync_per_call / migration) ---

    def upsert_transcript(self, call_id: str, data: Dict[str, Any]) -> None:
        row = _transcript_row(call_id, data)
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO transcripts (call_id, pipeline_stage, data)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (call_id) DO UPDATE SET
                         pipeline_stage = EXCLUDED.pipeline_stage,
                         data = EXCLUDED.data""",
                    (row["call_id"], row["pipeline_stage"], row["data"]),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

    def upsert_analysis(self, call_id: str, data: Dict[str, Any]) -> None:
        row = _analysis_row(call_id, data)
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO analyses
                         (call_id, direction, manager_id, manager_name, role,
                          spam_probability, effective_call, intent, outcome,
                          summary, audio_seconds, call_date, src_number,
                          dst_number, key_questions, objections, analysis_error, data)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (call_id) DO UPDATE SET
                         direction        = EXCLUDED.direction,
                         manager_id       = EXCLUDED.manager_id,
                         manager_name     = EXCLUDED.manager_name,
                         role             = EXCLUDED.role,
                         data             = EXCLUDED.data,
                         spam_probability = EXCLUDED.spam_probability,
                         effective_call   = EXCLUDED.effective_call,
                         intent           = EXCLUDED.intent,
                         outcome          = EXCLUDED.outcome,
                         summary          = EXCLUDED.summary,
                         audio_seconds    = EXCLUDED.audio_seconds,
                         call_date        = EXCLUDED.call_date,
                         src_number       = EXCLUDED.src_number,
                         dst_number       = EXCLUDED.dst_number,
                         key_questions    = EXCLUDED.key_questions,
                         objections       = EXCLUDED.objections,
                         analysis_error   = EXCLUDED.analysis_error""",
                    (
                        row["call_id"],
                        row["direction"],
                        row["manager_id"],
                        row["manager_name"],
                        row["role"],
                        row["spam_probability"],
                        row["effective_call"],
                        row["intent"],
                        row["outcome"],
                        row["summary"],
                        row["audio_seconds"],
                        row["call_date"],
                        row["src_number"],
                        row["dst_number"],
                        row["key_questions"],
                        row["objections"],
                        row["analysis_error"],
                        row["data"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

    def sync_per_call(self, per_call: list) -> None:
        """Bulk-sync a pipeline's per_call results into Postgres."""
        for item in per_call:
            if item.get("status") != "processed":
                continue
            call_id = item.get("meta", {}).get("call_id")
            if not call_id:
                continue
            self.upsert_analysis(call_id, item.get("analysis", {}))
