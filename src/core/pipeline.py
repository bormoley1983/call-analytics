import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from tqdm import tqdm

from adapters.storage_postgres import PostgresStorage, parse_call_datetime
from core.planner import categorize_files, discover_and_filter_files
from core.rules import ensure_analysis_schema, sha12
from core.stt_factory import build_stt_adapter
from core.stt_service import SttService
from core.transcript_fingerprint import transcript_text_sha256
from core.utils import sha256_file
from domain.config import AppConfig
from domain.stt import SttRequest
from ports.audio import AudioPort
from ports.llm import LlmPort
from ports.pbx import PbxPort
from ports.storage import StoragePort
from ports.stt import SttProcessorPort

logger = logging.getLogger(__name__)


def _progress_enabled() -> bool:
    return os.getenv("ENABLE_TQDM", "1") == "1" and sys.stderr.isatty()


class Pipeline:
    def __init__(
        self,
        config: AppConfig,
        storage: StoragePort,
        audio: AudioPort,
        llm: LlmPort,
        pbx: PbxPort,
        stt: SttProcessorPort | None = None,
    ):
        self.config = config
        self.storage = storage
        self.audio = audio
        self.llm = llm
        self.pbx = pbx
        self._stt = stt
        self._stt_service: SttService | None = None

    def _get_stt_service(self) -> SttService:
        if self._stt_service is None:
            processor = self._stt or build_stt_adapter(self.config)
            self._stt_service = SttService(processor, self.config)
        return self._stt_service

    def run(self) -> None:
        started_at = time.perf_counter()
        logger.info(
            "Pipeline starting: whisper=%s(%s/%s) ollama=%s workers=%d limit=%d",
            self.config.whisper_model,
            self.config.whisper_device,
            self.config.whisper_compute_type,
            self.config.ollama_model,
            self.config.analysis_workers,
            self.config.process_limit,
        )
        all_files = discover_and_filter_files(self.config, self.storage)
        if not all_files:
            logger.info("No files to process.")
            return

        needs_pipeline, analysis_only = categorize_files(
            all_files, self.config, self.storage
        )
        logger.info(
            "%d file(s) need pipeline, %d file(s) need analysis only",
            len(needs_pipeline),
            len(analysis_only),
        )

        files_metadata = self.run_transcription_phase(needs_pipeline)
        files_metadata = self.run_translation_phase(files_metadata)

        # Inject analysis-only files directly — skip Whisper and translation entirely
        for src in analysis_only:
            meta = self._build_meta(src)
            call_id = meta.get("call_id")
            if not call_id:
                meta["status"] = "skipped_too_small"
                self._record_call_metadata(
                    meta,
                    status="skipped_too_small",
                    error_message="file below min_bytes after transcription",
                )
                continue
            transcript = self.storage.load_transcript(call_id)
            meta["audio_seconds"] = transcript.get("call_meta", {}).get(
                "audio_seconds"
            ) or self.audio.duration_seconds(self.config.norm / f"{call_id}.wav")
            meta["status"] = "transcribed"
            meta["stage"] = "translated"
            files_metadata.append(meta)
            logger.info(
                "Queued for analysis-only: call_id=%s file=%s manager=%s",
                call_id,
                meta["source_file"],
                meta["manager_name"],
            )

        per_call = self.run_analysis_phase(files_metadata)
        self.sync_to_postgres(per_call)
        logger.info(
            "Snapshot export is decoupled from processing; use dedicated export flow when needed."
        )
        logger.info("Processing complete in %.2fs.", time.perf_counter() - started_at)

    def _build_meta(self, src: Path) -> dict[str, Any]:
        meta = self.pbx.parse_filename(src.name)
        meta["source_file"] = src.name
        meta["source_path"] = str(src)
        manager_info = self.config.manager_mapper.find_manager(
            meta.get("src_number", ""),
            meta.get("dst_number", ""),
            meta.get("direction", "unknown"),
        )
        meta["manager_name"] = manager_info["name"]
        meta["manager_id"] = manager_info["id"]
        meta["role"] = manager_info.get("role", "unknown")

        st_size = src.stat().st_size
        if st_size >= self.config.min_bytes:
            cid = sha12(src.name + str(st_size))
            meta["call_id"] = cid

        return meta

    def _record_call_metadata(
        self, meta: dict[str, Any], *, status: str, error_message: str | None = None
    ) -> None:
        upsert = getattr(self.storage, "upsert_call_metadata", None)
        if not callable(upsert):
            return
        call_id = meta.get("call_id")
        if not call_id:
            return
        upsert(
            call_id=call_id,
            source_file=meta.get("source_file"),
            source_path=meta.get("source_path"),
            call_datetime=parse_call_datetime(meta.get("date") or "", meta.get("time")),
            status=status,
            error_message=error_message,
        )

    def run_transcription_phase(self, files: list[Path]) -> list[dict[str, Any]]:
        """
        Phase 1: Transcription with Whisper (GPU intensive).
        Returns metadata for all files including skipped ones.
        """
        if not files:
            logger.info("No files to process.")
            return []

        logger.info("Phase 1: Transcription (Whisper) for %d file(s)", len(files))

        files_metadata: list[dict[str, Any]] = []

        stt_service = self._get_stt_service()
        try:
            for index, src in enumerate(
                tqdm(files, desc="Transcribing", disable=not _progress_enabled()),
                start=1,
            ):
                meta = self._build_meta(src)
                logger.info(
                    "[%d/%d] Preparing file=%s manager=%s direction=%s",
                    index,
                    len(files),
                    meta["source_file"],
                    meta["manager_name"],
                    meta.get("direction", "unknown"),
                )
                if "call_id" not in meta:
                    meta["status"] = "skipped_too_small"
                    files_metadata.append(meta)
                    logger.info(
                        "Skipping file=%s reason=too_small min_bytes=%d",
                        meta["source_file"],
                        self.config.min_bytes,
                    )
                    continue

                self._record_call_metadata(meta, status="discovered")

                dur = self.audio.duration_seconds(src)
                meta["audio_seconds"] = dur

                if dur < self.config.min_seconds:
                    meta["status"] = "skipped_too_short"
                    files_metadata.append(meta)
                    self._record_call_metadata(
                        meta,
                        status="skipped_too_short",
                        error_message="duration_below_min_seconds",
                    )
                    logger.info(
                        "Skipping call_id=%s file=%s reason=too_short duration=%.2fs min_seconds=%.2fs",
                        meta["call_id"],
                        meta["source_file"],
                        dur,
                        self.config.min_seconds,
                    )
                    continue

                call_id = meta["call_id"]
                norm_path = self.config.norm / f"{call_id}.wav"

                if not norm_path.exists():
                    logger.info(
                        "Normalizing audio: call_id=%s source=%s target=%s",
                        call_id,
                        src,
                        norm_path,
                    )
                    self.audio.normalize(src, norm_path)
                else:
                    logger.debug(
                        "Normalized audio already exists: call_id=%s path=%s",
                        call_id,
                        norm_path,
                    )

                # Transcribe
                transcript: dict[str, Any]
                newly_transcribed = False
                if (
                    not self.config.force_retranscribe
                ) and self.storage.transcript_exists(call_id):
                    transcript = self.storage.load_transcript(call_id)
                    logger.info(
                        "Reusing existing transcript: call_id=%s stage=%s",
                        call_id,
                        transcript.get("_pipeline_stage", "unknown"),
                    )
                else:
                    logger.info(
                        "Running STT transcription: call_id=%s duration=%.2fs provider=%s model=%s",
                        call_id,
                        dur,
                        stt_service.identity.provider,
                        stt_service.identity.model_id,
                    )
                    request = SttRequest(
                        call_id=call_id,
                        audio_path=norm_path,
                        audio_seconds=dur,
                        audio_sha256=sha256_file(norm_path),
                        language=self.config.stt_language,
                    )
                    transcribe_started = time.perf_counter()
                    transcript = stt_service.transcribe_one(request)
                    newly_transcribed = True
                    logger.info(
                        "STT complete: call_id=%s segments=%d text_chars=%d elapsed=%.2fs",
                        call_id,
                        len(transcript.get("segments", [])),
                        len(transcript.get("text", "")),
                        time.perf_counter() - transcribe_started,
                    )

                # Add manager info to transcript
                transcript["manager_name"] = meta["manager_name"]
                transcript["manager_id"] = meta["manager_id"]
                transcript["role"] = meta["role"]
                transcript["call_meta"] = {
                    "direction": meta.get("direction"),
                    "src_number": meta.get("src_number"),
                    "dst_number": meta.get("dst_number"),
                    "date": meta.get("date"),
                    "time": meta.get("time"),
                }

                # Save immediately after STT so a crash during translation
                # doesn't require re-running GPU transcription
                if newly_transcribed or self.config.force_retranscribe:
                    transcript["_pipeline_stage"] = "transcribed"
                    self.storage.save_transcript(call_id, transcript)
                    logger.debug(
                        "Saved transcript snapshot: call_id=%s stage=transcribed",
                        call_id,
                    )

                meta["stage"] = transcript.get("_pipeline_stage", "transcribed")
                meta["status"] = "transcribed"
                self._record_call_metadata(meta, status="transcribed")
                files_metadata.append(meta)
                logger.info(
                    "File ready for analysis: call_id=%s file=%s stage=%s",
                    call_id,
                    meta["source_file"],
                    meta["stage"],
                )
        finally:
            stt_service.close()

        transcribed_count = len(
            [m for m in files_metadata if m.get("status") == "transcribed"]
        )
        logger.info("Transcription complete. Processed %d file(s).", transcribed_count)

        return files_metadata

    def run_translation_phase(
        self, files_metadata: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        logger.info("Phase 1b: Translation (Ollama after Whisper release)")

        for meta in files_metadata:
            if meta.get("status") != "transcribed":
                continue

            call_id = meta["call_id"]
            transcript = self.storage.load_transcript(call_id)
            completed_stage = transcript.get("_pipeline_stage", "transcribed")
            need_translate = (
                completed_stage != "translated"
                or self.config.force_retranscribe
                or self.config.force_translate_uk
            )

            if not need_translate:
                logger.info(
                    "Skipping translation: call_id=%s file=%s stage=%s",
                    call_id,
                    meta["source_file"],
                    completed_stage,
                )
                meta["stage"] = completed_stage
                continue

            segments = transcript.get("segments", [])
            try:
                logger.info(
                    "Translating transcript: call_id=%s segments=%d force_translate_uk=%s",
                    call_id,
                    len(segments),
                    self.config.force_translate_uk,
                )
                translate_started = time.perf_counter()
                translated = self.llm.translate_segments_to_uk(segments)
                if translated:
                    transcript["text_uk"] = "\n".join(translated)
                    transcript["segments_uk"] = [
                        {"start": seg["start"], "end": seg["end"], "text": uk}
                        for seg, uk in zip(segments, translated)
                    ]
                    logger.info(
                        "Translation complete: call_id=%s translated_segments=%d elapsed=%.2fs",
                        call_id,
                        len(translated),
                        time.perf_counter() - translate_started,
                    )
                else:
                    transcript.setdefault("text_uk", transcript.get("text", ""))
                    transcript.setdefault("segments_uk", segments)
                    logger.info(
                        "Translation skipped or fell back to source text: call_id=%s text_uk_chars=%d",
                        call_id,
                        len(transcript.get("text_uk", "")),
                    )
                transcript["_pipeline_stage"] = "translated"
                self.storage.save_transcript(call_id, transcript)
            except (RuntimeError, OSError) as e:
                transcript["text_uk"] = transcript.get("text", "")
                transcript["segments_uk"] = transcript.get("segments", [])
                transcript["translation_error"] = repr(e)
                transcript["needs_retranslation"] = True
                transcript["_pipeline_stage"] = "translated"
                self.storage.save_transcript(call_id, transcript)
                logger.warning(
                    "Translation failed, stored fallback transcript: call_id=%s error=%r",
                    call_id,
                    e,
                )

            meta["stage"] = transcript.get("_pipeline_stage", "transcribed")
            self._record_call_metadata(meta, status="translated")

        return files_metadata

    def run_analysis_phase(
        self, files_metadata: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        logger.info("Phase 2: Analysis (Ollama)")
        per_call: list[dict[str, Any]] = []
        to_analyze = [m for m in files_metadata if m.get("status") == "transcribed"]

        def _analyze_one(meta: dict[str, Any]) -> dict[str, Any]:
            call_id = meta["call_id"]
            transcript = self.storage.load_transcript(call_id)
            active_text_hash = transcript_text_sha256(transcript)
            self.storage.mark_analysis_stale_if_text_changed(call_id, active_text_hash)
            if (not self.config.force_reanalyze) and self.storage.analysis_exists(
                call_id
            ):
                analysis = self.storage.load_analysis(call_id)
                analysis = ensure_analysis_schema(analysis, meta)
                logger.info(
                    "Reusing existing analysis: call_id=%s file=%s",
                    call_id,
                    meta["source_file"],
                )
            else:
                text_uk = (
                    transcript.get("text_uk") or transcript.get("text") or ""
                ).strip()
                try:
                    logger.info(
                        "Running analysis: call_id=%s file=%s text_chars=%d",
                        call_id,
                        meta["source_file"],
                        len(text_uk),
                    )
                    analysis_started = time.perf_counter()
                    analysis = self.llm.analyze(meta, text_uk)
                    logger.info(
                        "Analysis complete: call_id=%s intent=%s outcome=%s spam_probability=%s elapsed=%.2fs",
                        call_id,
                        analysis.get("intent"),
                        analysis.get("outcome"),
                        analysis.get("spam_probability"),
                        time.perf_counter() - analysis_started,
                    )
                except (RuntimeError, TypeError) as e:
                    analysis = ensure_analysis_schema({}, meta)
                    analysis.update(
                        {
                            "effective_call": False,
                            "spam_probability": 1.0,
                            "intent": "інше",
                            "outcome": "невідомо",
                            "summary": "Не вдалося отримати коректний JSON-аналіз від моделі.",
                            "analysis_error": repr(e),
                        }
                    )
                    logger.warning(
                        "Analysis failed, stored fallback result: call_id=%s error=%r",
                        call_id,
                        e,
                    )
            analysis.update(
                {
                    "manager_name": meta["manager_name"],
                    "manager_id": meta["manager_id"],
                    "role": meta["role"],
                    "input_text_sha256": active_text_hash,
                    "call_meta": {
                        "direction": meta.get("direction"),
                        "src_number": meta.get("src_number"),
                        "dst_number": meta.get("dst_number"),
                        "date": meta.get("date"),
                        "time": meta.get("time"),
                        "audio_seconds": meta.get("audio_seconds"),
                        "source_file": meta.get("source_file"),
                        "source_path": meta.get("source_path"),
                    },
                }
            )

            # Use atomic save when available (Postgres) to ensure transcript,
            # analysis, and calls metadata are committed together. Falls back
            # to individual saves for JSON storage or other adapters.
            save_atomically = getattr(self.storage, "save_call_atomically", None)
            if callable(save_atomically):
                try:
                    save_atomically(call_id, transcript, analysis)
                except (RuntimeError, OSError) as exc:
                    logger.error(
                        "Atomic save failed for call_id=%s: %s — falling back to individual saves",
                        call_id,
                        exc,
                    )
                    self.storage.save_analysis(call_id, analysis)
            else:
                self.storage.save_analysis(call_id, analysis)

            self._record_call_metadata(
                meta,
                status="processed",
                error_message=str(analysis.get("analysis_error") or "").strip() or None,
            )
            logger.debug(
                "Saved analysis: call_id=%s file=%s", call_id, meta["source_file"]
            )
            return {"meta": meta, "analysis": analysis, "status": "processed"}

        workers = self.config.analysis_workers

        # Warn if analysis workers may exceed the PostgreSQL connection pool.
        # Each worker thread needs a connection from the pool; when workers > max_connections
        # threads block waiting for an available connection, which throttles throughput.
        pg_pool_max = getattr(self.storage, "max_connections", None)
        if isinstance(self.storage, PostgresStorage) and pg_pool_max is not None:
            if workers > pg_pool_max:
                logger.warning(
                    "analysis_workers (%d) exceeds PG_POOL_MAX (%d) — workers will block "
                    "waiting for connections. Increase PG_POOL_MAX or reduce ANALYSIS_WORKERS.",
                    workers,
                    pg_pool_max,
                )
            else:
                logger.info(
                    "analysis_workers=%d within PG_POOL_MAX=%d", workers, pg_pool_max
                )

        logger.info(
            "Analysis workers: %d; queued call(s): %d", workers, len(to_analyze)
        )

        if workers == 1:
            for meta in tqdm(
                to_analyze, desc="Analyzing", disable=not _progress_enabled()
            ):
                per_call.append(_analyze_one(meta))
        else:
            # Safety timeout per worker: Ollama generation timeout + 120s buffer
            # for DB reads/writes and text processing. Prevents a hung worker
            # from blocking the pipeline indefinitely.
            per_worker_timeout = getattr(self.config, "ollama_timeout", 600) + 120
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_analyze_one, m): m for m in to_analyze}
                for fut in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="Analyzing",
                    disable=not _progress_enabled(),
                ):
                    try:
                        per_call.append(fut.result(timeout=per_worker_timeout))
                    except Exception as e:  # noqa: BLE001 -- outermost pipeline loop; must not let a single call crash the batch
                        logger.error(
                            "Analysis failed for %s: %s",
                            futures[fut].get("source_file"),
                            e,
                        )

        for meta in files_metadata:
            if meta.get("status") in ("skipped_too_small", "skipped_too_short"):
                per_call.append({"meta": meta, "status": meta["status"]})

        logger.info(
            "Analysis complete. Processed %d call(s).",
            len([c for c in per_call if c.get("status") == "processed"]),
        )
        return per_call

    def sync_to_postgres(self, per_call: list[dict[str, Any]]) -> None:
        if isinstance(self.storage, PostgresStorage):
            return
        dsn = os.getenv("POSTGRES_DSN", "")
        if not dsn:
            return
        logger.info("Syncing processed results to Postgres")
        pg = PostgresStorage(dsn)
        pg.ensure_ready()
        try:
            synced = 0
            for item in per_call:
                meta = item.get("meta", {}) or {}
                call_id = meta.get("call_id")
                if call_id:
                    pg.upsert_call_metadata(
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
                pg.upsert_transcript(call_id, self.storage.load_transcript(call_id))
                pg.upsert_analysis(call_id, item.get("analysis", {}))
                pg.upsert_call_metadata(
                    call_id=call_id,
                    source_file=meta.get("source_file"),
                    source_path=meta.get("source_path"),
                    call_datetime=parse_call_datetime(
                        meta.get("date") or "", meta.get("time")
                    ),
                    status="processed",
                    error_message=str(
                        (item.get("analysis", {}) or {}).get("analysis_error") or ""
                    ).strip()
                    or None,
                    mark_synced=True,
                )
                synced += 1
            logger.info("Postgres sync complete: %d call(s) upserted", synced)
        finally:
            pg.close()
