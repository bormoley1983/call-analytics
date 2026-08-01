from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from psycopg2 import extensions as pg_extensions
from psycopg2 import pool as pg_pool
from psycopg2.extras import Json

from adapters.stt_runs_schema import STT_RUNS_DDL


def parse_call_datetime(
    date_str: str, time_str: Optional[str] = None
) -> Optional[datetime]:
    """Parse PBX date (YYYYMMDD) and optional time (HHMMSS) into a timezone-aware datetime.

    Uses UTC as the default timezone since PBX systems typically report in UTC.
    Returns None if date_str is empty or invalid.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        if time_str:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        # PBX systems typically report in UTC; attach UTC timezone for TIMESTAMPTZ
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


DDL = r"""
CREATE TABLE IF NOT EXISTS transcripts (
    call_id             TEXT PRIMARY KEY,
    pipeline_stage       TEXT,
    stt_run_id           UUID,
    stt_config_hash      TEXT,
    source_text_sha256   TEXT,
    data                JSONB NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT now()
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
    call_datetime   TIMESTAMPTZ,
    src_number       TEXT,
    dst_number       TEXT,
    key_questions    JSONB,
    objections       JSONB,
    analysis_error   TEXT,
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS calls (
    call_id         TEXT PRIMARY KEY,
    source_file     TEXT,
    source_path     TEXT,
    call_datetime   TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'discovered',
    error_message   TEXT,
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    transcribed_at  TIMESTAMPTZ,
    translated_at   TIMESTAMPTZ,
    analyzed_at     TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
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
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS stt_run_id UUID;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS stt_config_hash TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS source_text_sha256 TEXT;
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
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS src_number TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS dst_number TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS key_questions JSONB;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS objections JSONB;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS analysis_error TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS source_path TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS discovered_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS transcribed_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS translated_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS analyzed_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
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

-- Migration: add call_datetime TIMESTAMPTZ, backfill from call_date (YYYYMMDD) if it exists, then drop call_date
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS call_datetime TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_datetime TIMESTAMPTZ;

-- Safe backfill: only run if call_date column still exists
DO $$
BEGIN
  -- Backfill analyses
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='analyses' AND column_name='call_date') THEN
    UPDATE analyses SET call_datetime = to_timestamp(call_date, 'YYYYMMDD') AT TIME ZONE 'UTC'
    WHERE call_datetime IS NULL AND call_date ~ '^\d{8}$';
  END IF;
  -- Backfill calls
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='calls' AND column_name='call_date') THEN
    UPDATE calls SET call_datetime = to_timestamp(call_date, 'YYYYMMDD') AT TIME ZONE 'UTC'
    WHERE call_datetime IS NULL AND call_date ~ '^\d{8}$';
  END IF;
END $$;

ALTER TABLE analyses DROP COLUMN IF EXISTS call_date;
ALTER TABLE calls DROP COLUMN IF EXISTS call_date;

CREATE INDEX IF NOT EXISTS idx_analyses_manager_id ON analyses(manager_id);
CREATE INDEX IF NOT EXISTS idx_analyses_role ON analyses(role);
CREATE INDEX IF NOT EXISTS idx_analyses_intent ON analyses(intent);
CREATE INDEX IF NOT EXISTS idx_analyses_outcome ON analyses(outcome);
CREATE INDEX IF NOT EXISTS idx_analyses_direction ON analyses(direction);
CREATE INDEX IF NOT EXISTS idx_analyses_filter_path ON analyses(call_datetime, manager_id, role, direction);
CREATE INDEX IF NOT EXISTS idx_analyses_effective_filter ON analyses(call_datetime, manager_id) WHERE effective_call IS TRUE;
CREATE INDEX IF NOT EXISTS idx_analyses_spam_filter ON analyses(spam_probability, call_datetime) WHERE spam_probability IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status);
CREATE INDEX IF NOT EXISTS idx_calls_updated_at ON calls(updated_at DESC);
-- New indexes on call_datetime for efficient temporal queries
CREATE INDEX IF NOT EXISTS idx_analyses_call_datetime ON analyses(call_datetime);
CREATE INDEX IF NOT EXISTS idx_calls_call_datetime ON calls(call_datetime);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_id ON call_keywords(keyword_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_call_id ON call_keywords(call_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_call ON call_keywords(keyword_id, call_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_match_sort ON call_keywords(keyword_id, match_count DESC, call_id DESC);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analyses_created_at ON keyword_ai_analyses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analysis_items_analysis_id ON keyword_ai_analysis_items(analysis_id);

-- AI Apply table: track applied actions from catalog analyses with audit metadata
CREATE TABLE IF NOT EXISTS ai_apply (
    apply_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id       TEXT NOT NULL REFERENCES keyword_ai_analyses(analysis_id) ON DELETE CASCADE,
    applied_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by        TEXT DEFAULT current_user,
    dry_run           BOOLEAN NOT NULL DEFAULT false,
    actions_applied   JSONB NOT NULL DEFAULT '[]'::jsonb,
    actions_skipped   JSONB NOT NULL DEFAULT '[]'::jsonb,
    mutations         JSONB NOT NULL DEFAULT '[]'::jsonb,
    keyword_refreshed BOOLEAN NOT NULL DEFAULT false,
    follow_up_ran     BOOLEAN NOT NULL DEFAULT false,
    error             TEXT
);

CREATE INDEX IF NOT EXISTS idx_ai_apply_analysis_id ON ai_apply(analysis_id);
CREATE INDEX IF NOT EXISTS idx_ai_apply_applied_at ON ai_apply(applied_at);

-- AI Alias Suggestions: track AI-suggested aliases with provenance
CREATE TABLE IF NOT EXISTS ai_alias_suggestions (
    suggestion_id     UUID PRIMARY KEY,
    keyword_id        TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    suggested_aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_evidence   JSONB,
    ai_model          TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_alias_suggestions_keyword_status
    ON ai_alias_suggestions(keyword_id, status);

-- Deep Insights Runs: track deep insights generation runs
CREATE TABLE IF NOT EXISTS ai_deep_insights_runs (
    run_id        UUID PRIMARY KEY,
    ai_model      TEXT,
    insight_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    request_data  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_runs_created_at
    ON ai_deep_insights_runs(created_at DESC);

-- Deep Insights: individual insights from a run
CREATE TABLE IF NOT EXISTS ai_deep_insights (
    insight_id            UUID PRIMARY KEY,
    run_id                UUID NOT NULL REFERENCES ai_deep_insights_runs(run_id) ON DELETE CASCADE,
    insight_type          TEXT NOT NULL,
    title                 TEXT NOT NULL DEFAULT '',
    description           TEXT NOT NULL DEFAULT '',
    severity              TEXT NOT NULL DEFAULT 'low',
    affected_calls_count  INTEGER NOT NULL DEFAULT 0,
    evidence_summary      TEXT,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_run_id ON ai_deep_insights(run_id);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_type_severity
    ON ai_deep_insights(insight_type, severity);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_created_at
    ON ai_deep_insights(created_at DESC);
"""

DDL += "\n" + STT_RUNS_DDL


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
        effective_call = effective_call.strip().lower() in {
            "1",
            "true",
            "yes",
            "tak",
            "так",
        }
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
        "call_datetime": parse_call_datetime(
            call_meta.get("date") or "",
            call_meta.get("time"),
        ),
        "src_number": call_meta.get("src_number"),
        "dst_number": call_meta.get("dst_number"),
        "key_questions": _jsonb(data.get("key_questions") or []),
        "objections": _jsonb(data.get("objections") or []),
        "analysis_error": data.get("analysis_error"),
        "input_text_sha256": data.get("input_text_sha256"),
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
            raise RuntimeError(
                "PostgresStorage is not initialized. Call ensure_ready() first."
            )
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

    def mark_analysis_stale_if_text_changed(
        self, call_id: str, text_sha256: str
    ) -> bool:
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT input_text_sha256 FROM analyses WHERE call_id = %s",
                    (call_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                old_hash = row[0] or ""
                if old_hash == text_sha256:
                    return False
                cur.execute("DELETE FROM analyses WHERE call_id = %s", (call_id,))
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

    def promote_stt_result(
        self,
        call_id: str,
        transcript: Dict[str, Any],
        *,
        stt_run_id: str,
        stt_config_hash: str,
        source_text_sha256: str,
    ) -> None:
        promoted = dict(transcript)
        promoted["stt_run_id"] = stt_run_id
        promoted["stt_config_hash"] = stt_config_hash
        promoted["source_text_sha256"] = source_text_sha256

        row = _transcript_row(call_id, promoted)
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO transcripts (call_id, pipeline_stage, stt_run_id, stt_config_hash, source_text_sha256, data)
                    VALUES (%s, %s, %s::uuid, %s, %s, %s)
                    ON CONFLICT (call_id) DO UPDATE SET
                      pipeline_stage = EXCLUDED.pipeline_stage,
                      stt_run_id = EXCLUDED.stt_run_id,
                      stt_config_hash = EXCLUDED.stt_config_hash,
                      source_text_sha256 = EXCLUDED.source_text_sha256,
                      data = EXCLUDED.data
                    """,
                    (
                        call_id,
                        row["pipeline_stage"],
                        stt_run_id,
                        stt_config_hash,
                        source_text_sha256,
                        row["data"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

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
                raw_call_meta = data.get("call_meta")
                call_meta: dict[str, Any] = (
                    raw_call_meta if isinstance(raw_call_meta, dict) else {}
                )
                stage = row["pipeline_stage"] or "transcribed"
                status = "translated" if stage == "translated" else "transcribed"
                transcribed_at = (
                    "now()"
                    if status in {"transcribed", "translated", "processed"}
                    else "NULL"
                )
                translated_at = (
                    "now()" if status in {"translated", "processed"} else "NULL"
                )
                call_dt = parse_call_datetime(
                    call_meta.get("date") or "", call_meta.get("time")
                )
                cur.execute(
                    f"""
                    INSERT INTO calls
                        (call_id, call_datetime, status, transcribed_at, translated_at, updated_at)
                    VALUES
                        (%s, %s, %s, {transcribed_at}, {translated_at}, now())
                    ON CONFLICT (call_id) DO UPDATE SET
                        call_datetime = COALESCE(EXCLUDED.call_datetime, calls.call_datetime),
                        status = EXCLUDED.status,
                        transcribed_at = COALESCE(calls.transcribed_at, EXCLUDED.transcribed_at),
                        translated_at = COALESCE(calls.translated_at, EXCLUDED.translated_at),
                        updated_at = now()
                    """,
                    (call_id, call_dt, status),
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
                          summary, audio_seconds, call_datetime, src_number,
                                  dst_number, key_questions, objections, analysis_error, input_text_sha256, data)
                              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                         call_datetime    = EXCLUDED.call_datetime,
                         src_number       = EXCLUDED.src_number,
                         dst_number       = EXCLUDED.dst_number,
                         key_questions    = EXCLUDED.key_questions,
                         objections       = EXCLUDED.objections,
                         analysis_error   = EXCLUDED.analysis_error,
                         input_text_sha256 = EXCLUDED.input_text_sha256,
                         data             = EXCLUDED.data""",
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
                        row["call_datetime"],
                        row["src_number"],
                        row["dst_number"],
                        row["key_questions"],
                        row["objections"],
                        row["analysis_error"],
                        row["input_text_sha256"],
                        row["data"],
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO calls
                        (call_id, source_file, source_path, call_datetime, status, error_message, analyzed_at, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (call_id) DO UPDATE SET
                        source_file = COALESCE(EXCLUDED.source_file, calls.source_file),
                        source_path = COALESCE(EXCLUDED.source_path, calls.source_path),
                        call_datetime = COALESCE(EXCLUDED.call_datetime, calls.call_datetime),
                        status = EXCLUDED.status,
                        error_message = COALESCE(NULLIF(EXCLUDED.error_message, ''), calls.error_message),
                        analyzed_at = COALESCE(calls.analyzed_at, EXCLUDED.analyzed_at),
                        updated_at = now()
                    """,
                    (
                        call_id,
                        row.get("source_file"),
                        row.get("source_path"),
                        row["call_datetime"],
                        "processed",
                        row["analysis_error"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

    def upsert_call_metadata(
        self,
        *,
        call_id: str,
        source_file: str | None = None,
        source_path: str | None = None,
        call_datetime: datetime | None = None,
        status: str = "discovered",
        error_message: str | None = None,
        mark_synced: bool = False,
    ) -> None:
        stage_values = {
            "transcribed_at": (
                "now()"
                if status in {"transcribed", "translated", "processed"}
                else "NULL"
            ),
            "translated_at": (
                "now()" if status in {"translated", "processed"} else "NULL"
            ),
            "analyzed_at": "now()" if status == "processed" else "NULL",
            "synced_at": "now()" if mark_synced else "NULL",
        }

        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO calls (
                        call_id,
                        source_file,
                        source_path,
                        call_datetime,
                        status,
                        error_message,
                        {', '.join(stage_values.keys())},
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        {', '.join(stage_values.values())},
                        now()
                    )
                    ON CONFLICT (call_id) DO UPDATE SET
                        source_file = COALESCE(EXCLUDED.source_file, calls.source_file),
                        source_path = COALESCE(EXCLUDED.source_path, calls.source_path),
                        call_datetime = COALESCE(EXCLUDED.call_datetime, calls.call_datetime),
                        status = EXCLUDED.status,
                        error_message = COALESCE(NULLIF(EXCLUDED.error_message, ''), calls.error_message),
                        transcribed_at = COALESCE(calls.transcribed_at, EXCLUDED.transcribed_at),
                        translated_at = COALESCE(calls.translated_at, EXCLUDED.translated_at),
                        analyzed_at = COALESCE(calls.analyzed_at, EXCLUDED.analyzed_at),
                        synced_at = COALESCE(EXCLUDED.synced_at, calls.synced_at),
                        updated_at = now()
                    """,
                    (
                        call_id,
                        source_file,
                        source_path,
                        call_datetime,
                        status,
                        error_message,
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
            meta = item.get("meta", {}) or {}
            call_id = meta.get("call_id")
            if call_id:
                self.upsert_call_metadata(
                    call_id=call_id,
                    source_file=meta.get("source_file"),
                    source_path=meta.get("source_path"),
                    call_datetime=parse_call_datetime(
                        meta.get("date") or "", meta.get("time")
                    ),
                    status=item.get("status") or "discovered",
                    error_message=(
                        "duration_below_min_seconds"
                        if item.get("status") == "skipped_too_short"
                        else None
                    ),
                )

            if item.get("status") != "processed":
                continue
            if not call_id:
                continue
            self.upsert_analysis(call_id, item.get("analysis", {}))

    # --- test-friendly helpers ---

    def store_call(
        self,
        *,
        call_id: str,
        status: str = "discovered",
        source_file: str | None = None,
        source_path: str | None = None,
        call_datetime: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Convenience wrapper around upsert_call_metadata for test fixtures."""
        self.upsert_call_metadata(
            call_id=call_id,
            source_file=source_file,
            source_path=source_path,
            call_datetime=call_datetime,
            status=status,
            error_message=error_message,
        )

    def get_calls(self, call_ids: list[str]) -> list[Dict[str, Any]]:
        """Return call rows for the given call IDs."""
        placeholders = ",".join(["%s"] * len(call_ids))
        sql = f"""
            SELECT call_id, source_file, source_path, call_datetime, status, error_message,
                   discovered_at, transcribed_at, translated_at, analyzed_at, synced_at,
                   created_at, updated_at
            FROM calls
            WHERE call_id IN ({placeholders})
        """
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, call_ids)
                desc = cur.description
                if desc is None:
                    return []
                columns = [d[0] for d in desc]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            self._putconn(conn)
