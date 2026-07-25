from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from psycopg2.extras import Json

from adapters.postgres_single_connection import SingleConnectionPostgresAdapter
from adapters.stt_runs_schema import STT_RUNS_DDL
from domain.stt_runs import SttRunManifest, SttRunResultRecord
from ports.stt_runs import SttRunStorePort


def _jsonb(value: Any) -> Json:
    return Json(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value)
    return None


class PostgresSttRunStore(SingleConnectionPostgresAdapter, SttRunStorePort):
    def _initialize_connection(self, conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute(STT_RUNS_DDL)
        conn.commit()

    def ensure_ready(self) -> None:
        self._getconn()

    def create_run(self, run: SttRunManifest) -> None:
        def _write(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stt_runs (
                        run_id, name, purpose, provider, model_id, model_revision,
                        config_hash, config_data, code_revision, dataset_manifest_hash,
                        hardware_data, status, total_calls, completed_calls,
                        failed_calls, skipped_calls, total_audio_seconds,
                        model_load_seconds, inference_seconds, wall_seconds,
                        started_at, finished_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO NOTHING
                    """,
                    (
                        run.run_id,
                        run.name,
                        run.purpose,
                        run.provider,
                        run.model_id,
                        run.model_revision,
                        run.config_hash,
                        _jsonb(run.config_data),
                        run.code_revision,
                        run.dataset_manifest_hash,
                        _jsonb(run.hardware_data),
                        run.status,
                        run.total_calls,
                        run.completed_calls,
                        run.failed_calls,
                        run.skipped_calls,
                        run.total_audio_seconds,
                        run.model_load_seconds,
                        run.inference_seconds,
                        run.wall_seconds,
                        run.started_at,
                        run.finished_at,
                    ),
                )

        self._run_write(_write)

    def update_run_status(self, run_id: str, status: str) -> None:
        def _write(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute("UPDATE stt_runs SET status = %s WHERE run_id = %s", (status, run_id))

        self._run_write(_write)

    def update_run_counters(
        self,
        run_id: str,
        *,
        total_calls: int,
        completed_calls: int,
        failed_calls: int,
        skipped_calls: int,
    ) -> None:
        def _write(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE stt_runs
                    SET total_calls = %s,
                        completed_calls = %s,
                        failed_calls = %s,
                        skipped_calls = %s
                    WHERE run_id = %s
                    """,
                    (total_calls, completed_calls, failed_calls, skipped_calls, run_id),
                )

        self._run_write(_write)

    def upsert_result(self, result: SttRunResultRecord) -> None:
        def _write(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stt_results (
                        run_id, call_id, audio_sha256, audio_seconds, status,
                        raw_payload, canonical_payload, text_sha256,
                        elapsed_seconds, rtf, peak_vram_mb, batch_size,
                        retry_count, warnings, error_category, error_detail
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, call_id) DO UPDATE SET
                        audio_sha256 = EXCLUDED.audio_sha256,
                        audio_seconds = EXCLUDED.audio_seconds,
                        status = EXCLUDED.status,
                        raw_payload = EXCLUDED.raw_payload,
                        canonical_payload = EXCLUDED.canonical_payload,
                        text_sha256 = EXCLUDED.text_sha256,
                        elapsed_seconds = EXCLUDED.elapsed_seconds,
                        rtf = EXCLUDED.rtf,
                        peak_vram_mb = EXCLUDED.peak_vram_mb,
                        batch_size = EXCLUDED.batch_size,
                        retry_count = EXCLUDED.retry_count,
                        warnings = EXCLUDED.warnings,
                        error_category = EXCLUDED.error_category,
                        error_detail = EXCLUDED.error_detail,
                        updated_at = now()
                    """,
                    (
                        result.run_id,
                        result.call_id,
                        result.audio_sha256,
                        result.audio_seconds,
                        result.status,
                        _jsonb(result.raw_payload),
                        _jsonb(result.canonical_payload),
                        result.text_sha256,
                        result.elapsed_seconds,
                        result.rtf,
                        result.peak_vram_mb,
                        result.batch_size,
                        result.retry_count,
                        _jsonb(result.warnings),
                        result.error_category,
                        result.error_detail,
                    ),
                )

        self._run_write(_write)

    def load_run(self, run_id: str) -> SttRunManifest:
        def _read(conn: Any) -> SttRunManifest:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, name, purpose, provider, model_id, model_revision,
                           config_hash, config_data, code_revision, dataset_manifest_hash,
                           hardware_data, status, total_calls, completed_calls,
                           failed_calls, skipped_calls, total_audio_seconds,
                           model_load_seconds, inference_seconds, wall_seconds,
                           started_at, finished_at, created_at
                    FROM stt_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(f"STT run not found: {run_id}")
                return SttRunManifest(
                    run_id=row[0],
                    name=row[1],
                    purpose=row[2],
                    provider=row[3],
                    model_id=row[4],
                    model_revision=row[5],
                    config_hash=row[6],
                    config_data=row[7] or {},
                    code_revision=row[8] or "",
                    dataset_manifest_hash=row[9] or "",
                    hardware_data=row[10] or {},
                    status=row[11],
                    total_calls=row[12],
                    completed_calls=row[13],
                    failed_calls=row[14],
                    skipped_calls=row[15],
                    total_audio_seconds=row[16],
                    model_load_seconds=row[17],
                    inference_seconds=row[18],
                    wall_seconds=row[19],
                    started_at=_parse_datetime(row[20]),
                    finished_at=_parse_datetime(row[21]),
                    created_at=_parse_datetime(row[22]),
                )

        return self._run_read(_read)

    def load_result(self, run_id: str, call_id: str) -> SttRunResultRecord:
        def _read(conn: Any) -> SttRunResultRecord:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, call_id, audio_sha256, audio_seconds, status,
                           raw_payload, canonical_payload, text_sha256,
                           elapsed_seconds, rtf, peak_vram_mb, batch_size,
                           retry_count, warnings, error_category, error_detail,
                           created_at, updated_at
                    FROM stt_results
                    WHERE run_id = %s AND call_id = %s
                    """,
                    (run_id, call_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(f"STT result not found: {run_id}/{call_id}")
                return SttRunResultRecord(
                    run_id=row[0],
                    call_id=row[1],
                    audio_sha256=row[2],
                    audio_seconds=row[3],
                    status=row[4],
                    raw_payload=row[5] or {},
                    canonical_payload=row[6] or {},
                    text_sha256=row[7] or "",
                    elapsed_seconds=row[8],
                    rtf=row[9],
                    peak_vram_mb=row[10],
                    batch_size=row[11],
                    retry_count=row[12],
                    warnings=row[13] or [],
                    error_category=row[14],
                    error_detail=row[15],
                    created_at=_parse_datetime(row[16]),
                    updated_at=_parse_datetime(row[17]),
                )

        return self._run_read(_read)

    def iter_results(self, run_id: str) -> Iterator[SttRunResultRecord]:
        def _read(conn: Any) -> list[SttRunResultRecord]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, call_id, audio_sha256, audio_seconds, status,
                           raw_payload, canonical_payload, text_sha256,
                           elapsed_seconds, rtf, peak_vram_mb, batch_size,
                           retry_count, warnings, error_category, error_detail,
                           created_at, updated_at
                    FROM stt_results
                    WHERE run_id = %s
                    ORDER BY call_id
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()

            out: list[SttRunResultRecord] = []
            for row in rows:
                out.append(
                    SttRunResultRecord(
                        run_id=row[0],
                        call_id=row[1],
                        audio_sha256=row[2],
                        audio_seconds=row[3],
                        status=row[4],
                        raw_payload=row[5] or {},
                        canonical_payload=row[6] or {},
                        text_sha256=row[7] or "",
                        elapsed_seconds=row[8],
                        rtf=row[9],
                        peak_vram_mb=row[10],
                        batch_size=row[11],
                        retry_count=row[12],
                        warnings=row[13] or [],
                        error_category=row[14],
                        error_detail=row[15],
                        created_at=_parse_datetime(row[16]),
                        updated_at=_parse_datetime(row[17]),
                    )
                )
            return out

        for item in self._run_read(_read):
            yield item
