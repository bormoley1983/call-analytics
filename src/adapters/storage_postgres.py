from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from psycopg2 import extensions as pg_extensions
from psycopg2 import pool as pg_pool
from psycopg2.extras import Json

from adapters.migrations import apply_pending_migrations
from domain.call_datetime import parse_call_datetime  # noqa: F401  (re-export)

logger = logging.getLogger(__name__)


# Schema DDL lives exclusively in src/adapters/migrations/ (V001, V002).
# PostgresStorage.ensure_ready() applies pending migrations via
# apply_pending_migrations(); there is no inline DDL here anymore.



def _jsonb(value: Any) -> Json:
    return Json(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))


# Single source of truth for the analyses upsert (19 columns).
_ANALYSES_UPSERT_COLUMNS = (
    "call_id",
    "direction",
    "manager_id",
    "manager_name",
    "role",
    "spam_probability",
    "effective_call",
    "intent",
    "outcome",
    "summary",
    "audio_seconds",
    "call_datetime",
    "src_number",
    "dst_number",
    "key_questions",
    "objections",
    "analysis_error",
    "input_text_sha256",
    "data",
)

_ANALYSES_UPSERT_SQL = (
    "INSERT INTO analyses ({cols}) VALUES ({ph})\n"
    "ON CONFLICT (call_id) DO UPDATE SET\n"
    + ",\n".join(
        f"{col} = EXCLUDED.{col}" for col in _ANALYSES_UPSERT_COLUMNS if col != "call_id"
    )
).format(
    cols=", ".join(_ANALYSES_UPSERT_COLUMNS),
    ph=",".join(["%s"] * len(_ANALYSES_UPSERT_COLUMNS)),
)


def _analysis_upsert_params(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row[col] for col in _ANALYSES_UPSERT_COLUMNS)


def _ensure_utf8_client_encoding(conn: Any) -> Any:
    if getattr(conn, "encoding", "").upper() != "UTF8":
        conn.set_client_encoding("UTF8")
    return conn


def _infer_transcript_stage(data: dict[str, Any]) -> str | None:
    stage = data.get("_pipeline_stage")
    if isinstance(stage, str) and stage:
        return stage
    if data.get("text_uk") or data.get("segments_uk"):
        return "translated"
    if data.get("text") or data.get("segments"):
        return "transcribed"
    return None


def _transcript_row(call_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "call_id": call_id,
        "pipeline_stage": _infer_transcript_stage(data),
        "data": _jsonb(data),
    }


def _analysis_row(call_id: str, data: dict[str, Any]) -> dict[str, Any]:
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
        "source_file": call_meta.get("source_file"),
        "source_path": call_meta.get("source_path"),
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

    Connection pool sizing (override via environment variables):
        PG_POOL_MIN: minimum idle connections (default: 1)
        PG_POOL_MAX: maximum connections (default: 10)
    """

    def __init__(
        self,
        dsn: str,
        min_connections: int | None = None,
        max_connections: int | None = None,
    ):
        from domain.config import get_pg_pool_max, get_pg_pool_min

        self.dsn = dsn
        self.min_connections = (
            min_connections if min_connections is not None else get_pg_pool_min()
        )
        self.max_connections = (
            max_connections if max_connections is not None else get_pg_pool_max()
        )
        self._pool: pg_pool.ThreadedConnectionPool | None = None

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
        if self.max_connections < self.min_connections:
            logger.warning(
                "PG_POOL_MAX (%d) < PG_POOL_MIN (%d), adjusting max to %d",
                self.max_connections,
                self.min_connections,
                self.min_connections,
            )
            self.max_connections = self.min_connections
        self._pool = pg_pool.ThreadedConnectionPool(
            self.min_connections, self.max_connections, self.dsn
        )
        conn = self._getconn()
        try:
            apply_pending_migrations(conn)
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

    def load_transcript(self, call_id: str) -> dict[str, Any]:
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

    def load_analysis(self, call_id: str) -> dict[str, Any]:
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

    def save_transcript(self, call_id: str, data: dict[str, Any]) -> None:
        self.upsert_transcript(call_id, data)

    def save_analysis(self, call_id: str, data: dict[str, Any]) -> None:
        self.upsert_analysis(call_id, data)

    def save_call_atomically(
        self,
        call_id: str,
        transcript: dict[str, Any],
        analysis: dict[str, Any],
        call_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Atomically upsert transcript, analysis, and calls metadata in a single transaction.

        If any of the three upserts fail, the entire transaction rolls back so that
        no partial data is committed. This prevents inconsistent states where the
        transcript exists but the analysis does not (or vice versa).

        Args:
            call_id: Unique call identifier.
            transcript: Transcript data dict.
            analysis: Analysis data dict.
            call_metadata: Optional dict with source_file, source_path, date, time keys
                for the calls table upsert.
        """
        t_row = _transcript_row(call_id, transcript)
        a_row = _analysis_row(call_id, analysis)

        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                # 1. Upsert transcript
                cur.execute(
                    """INSERT INTO transcripts (call_id, pipeline_stage, data)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (call_id) DO UPDATE SET
                         pipeline_stage = EXCLUDED.pipeline_stage,
                         data = EXCLUDED.data""",
                    (t_row["call_id"], t_row["pipeline_stage"], t_row["data"]),
                )

                # 2. Upsert analysis
                cur.execute(_ANALYSES_UPSERT_SQL, _analysis_upsert_params(a_row))

                # 3. Upsert calls metadata
                call_meta = call_metadata or analysis.get("call_meta") or {}
                src_file = call_meta.get("source_file")
                src_path = call_meta.get("source_path")
                call_dt = parse_call_datetime(
                    call_meta.get("date") or "", call_meta.get("time")
                )

                cur.execute(
                    """
                    INSERT INTO calls (
                        call_id, source_file, source_path, call_datetime,
                        status, error_message, analyzed_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, now(), now())
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
                        src_file,
                        src_path,
                        call_dt,
                        "processed",
                        a_row["analysis_error"],
                    ),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._putconn(conn)

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
        transcript: dict[str, Any],
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

    def upsert_transcript(self, call_id: str, data: dict[str, Any]) -> None:
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

    def upsert_analysis(self, call_id: str, data: dict[str, Any]) -> None:
        row = _analysis_row(call_id, data)
        conn = self._getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(_ANALYSES_UPSERT_SQL, _analysis_upsert_params(row))
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
                        {", ".join(stage_values.keys())},
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        {", ".join(stage_values.values())},
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

    def get_calls(self, call_ids: list[str]) -> list[dict[str, Any]]:
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
