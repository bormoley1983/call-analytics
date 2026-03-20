import json
import logging
import os
import sys
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from faster_whisper import WhisperModel
from tqdm import tqdm

from adapters.reports_html import render_manager_report, render_overall_report
from adapters.storage_postgres import PostgresStorage
from core.planner import categorize_files, discover_and_filter_files
from core.reports import aggregate_report, aggregate_report_by_manager
from core.rules import ensure_analysis_schema, sha12
from core.transcription import transcribe
from domain.config import AppConfig
from ports.audio import AudioPort
from ports.llm import LlmPort
from ports.pbx import PbxPort
from ports.storage import StoragePort

logger = logging.getLogger(__name__)


def _progress_enabled() -> bool:
    return os.getenv("ENABLE_TQDM", "1") == "1" and sys.stderr.isatty()


class Pipeline:
    def __init__(self, config: AppConfig, storage: StoragePort, audio: AudioPort, llm: LlmPort, pbx: PbxPort):
        self.config = config
        self.storage = storage
        self.audio = audio
        self.llm = llm
        self.pbx = pbx

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
        
        needs_pipeline, analysis_only = categorize_files(all_files, self.config, self.storage)
        logger.info(
            "%d file(s) need pipeline, %d file(s) need analysis only",
            len(needs_pipeline), len(analysis_only),
        )

        files_metadata = self.run_transcription_phase(needs_pipeline)
        files_metadata = self.run_translation_phase(files_metadata)

        # Inject analysis-only files directly — skip Whisper and translation entirely
        for src in analysis_only:
            meta = self._build_meta(src)
            call_id = meta["call_id"]
            transcript = self.storage.load_transcript(call_id)
            meta["audio_seconds"] = transcript.get("call_meta", {}).get("audio_seconds") or \
                                    self.audio.duration_seconds(self.config.norm / f"{call_id}.wav")
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
        if self.config.generate_report_snapshots:
            self.generate_reports(per_call)
        else:
            logger.info("Snapshot report generation is disabled; skipping report artifacts.")
        logger.info("Processing complete in %.2fs.", time.perf_counter() - started_at)

    def _build_meta(self, src: Path) -> Dict[str, Any]:
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

    def run_transcription_phase(self, files: List[Path]) -> List[Dict[str, Any]]:
        """
        Phase 1: Transcription with Whisper (GPU intensive).
        Returns metadata for all files including skipped ones.
        """
        if not files:
            logger.info("No files to process.")
            return []

        logger.info("Phase 1: Transcription (Whisper) for %d file(s)", len(files))

        model = None
        files_metadata: List[Dict[str, Any]] = []

        for index, src in enumerate(tqdm(files, desc="Transcribing", disable=not _progress_enabled()), start=1):
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

            dur = self.audio.duration_seconds(src)
            meta["audio_seconds"] = dur

            if dur < self.config.min_seconds:
                meta["status"] = "skipped_too_short"
                files_metadata.append(meta)
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
                logger.info("Normalizing audio: call_id=%s source=%s target=%s", call_id, src, norm_path)
                self.audio.normalize(src, norm_path)
            else:
                logger.debug("Normalized audio already exists: call_id=%s path=%s", call_id, norm_path)

            # Transcribe
            transcript: Dict[str, Any]
            newly_transcribed = False
            if (not self.config.force_retranscribe) and self.storage.transcript_exists(call_id):
                transcript = self.storage.load_transcript(call_id)
                logger.info("Reusing existing transcript: call_id=%s stage=%s", call_id, transcript.get("_pipeline_stage", "unknown"))
            else:
                if model is None:
                    logger.info(
                        "Loading Whisper model: name=%s device=%s compute_type=%s",
                        self.config.whisper_model,
                        self.config.whisper_device,
                        self.config.whisper_compute_type,
                    )
                    model = WhisperModel(
                        self.config.whisper_model,
                        device=self.config.whisper_device,
                        compute_type=self.config.whisper_compute_type
                    )
                logger.info("Running Whisper transcription: call_id=%s duration=%.2fs", call_id, dur)
                transcribe_started = time.perf_counter()
                transcript = transcribe(model, norm_path, self.config)
                newly_transcribed = True
                logger.info(
                    "Whisper complete: call_id=%s segments=%d text_chars=%d elapsed=%.2fs",
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

            # Save immediately after Whisper so a crash during translation
            # doesn't require re-running the GPU transcription
            if newly_transcribed or self.config.force_retranscribe:
                transcript["_pipeline_stage"] = "transcribed"
                self.storage.save_transcript(call_id, transcript)
                logger.debug("Saved transcript snapshot: call_id=%s stage=transcribed", call_id)

            meta["stage"] = transcript.get("_pipeline_stage", "transcribed")
            meta["status"] = "transcribed"
            files_metadata.append(meta)
            logger.info(
                "File ready for analysis: call_id=%s file=%s stage=%s",
                call_id,
                meta["source_file"],
                meta["stage"],
            )

        # Free Whisper model from memory
        if model is not None:
            del model
            gc.collect()
            logger.info("Whisper model released from memory.")
        transcribed_count = len([m for m in files_metadata if m.get('status') == 'transcribed'])
        logger.info("Transcription complete. Processed %d file(s).", transcribed_count)
        
        return files_metadata


    def run_translation_phase(self, files_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
            except Exception as e:
                transcript["text_uk"] = transcript.get("text", "")
                transcript["segments_uk"] = transcript.get("segments", [])
                transcript["translation_error"] = repr(e)
                transcript["_pipeline_stage"] = "translated"
                self.storage.save_transcript(call_id, transcript)
                logger.warning("Translation failed, stored fallback transcript: call_id=%s error=%r", call_id, e)

            meta["stage"] = transcript.get("_pipeline_stage", "transcribed")

        return files_metadata


    def run_analysis_phase(self, files_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info("Phase 2: Analysis (Ollama)")
        per_call: List[Dict[str, Any]] = []
        to_analyze = [m for m in files_metadata if m.get("status") == "transcribed"]

        def _analyze_one(meta: Dict[str, Any]) -> Dict[str, Any]:
            call_id = meta["call_id"]
            transcript = self.storage.load_transcript(call_id)
            if (not self.config.force_reanalyze) and self.storage.analysis_exists(call_id):
                analysis = self.storage.load_analysis(call_id)
                analysis = ensure_analysis_schema(analysis, meta)
                logger.info("Reusing existing analysis: call_id=%s file=%s", call_id, meta["source_file"])
            else:
                text_uk = (transcript.get("text_uk") or transcript.get("text") or "").strip()
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
                except Exception as e:
                    analysis = ensure_analysis_schema({}, meta)
                    analysis.update({
                        "effective_call": False, "spam_probability": 1.0,
                        "intent": "інше", "outcome": "невідомо",
                        "summary": "Не вдалося отримати коректний JSON-аналіз від моделі.",
                        "analysis_error": repr(e),
                    })
                    logger.warning("Analysis failed, stored fallback result: call_id=%s error=%r", call_id, e)
            analysis.update({
                "manager_name": meta["manager_name"],
                "manager_id": meta["manager_id"],
                "role": meta["role"],
                "call_meta": {
                    "direction": meta.get("direction"),
                    "src_number": meta.get("src_number"),
                    "dst_number": meta.get("dst_number"),
                    "date": meta.get("date"),
                    "time": meta.get("time"),
                    "audio_seconds": meta.get("audio_seconds"),
                },
            })
            self.storage.save_analysis(call_id, analysis)
            logger.debug("Saved analysis: call_id=%s file=%s", call_id, meta["source_file"])
            return {"meta": meta, "analysis": analysis, "status": "processed"}

        workers = self.config.analysis_workers
        logger.info("Analysis workers: %d; queued call(s): %d", workers, len(to_analyze))

        if workers == 1:
            for meta in tqdm(to_analyze, desc="Analyzing", disable=not _progress_enabled()):
                per_call.append(_analyze_one(meta))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_analyze_one, m): m for m in to_analyze}
                for fut in tqdm(as_completed(futures), total=len(futures), desc="Analyzing", disable=not _progress_enabled()):
                    try:
                        per_call.append(fut.result())
                    except Exception as e:
                        logger.error("Analysis failed for %s: %s", futures[fut].get("source_file"), e)

        for meta in files_metadata:
            if meta.get("status") in ("skipped_too_small", "skipped_too_short"):
                per_call.append({"meta": meta, "status": meta["status"]})

        logger.info("Analysis complete. Processed %d call(s).",
                    len([c for c in per_call if c.get("status") == "processed"]))
        return per_call

    def generate_reports(self, per_call: List[Dict[str, Any]]) -> None:
        """Generate and save analysis reports."""
        logger.info("Generating reports for %d call result(s)", len(per_call))
        
        # Generate overall report
        report = aggregate_report(per_call, self.config)
        (self.config.out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Generate per-manager report
        manager_report = aggregate_report_by_manager(per_call, self.config)
        (self.config.out / "report_by_manager.json").write_text(json.dumps(manager_report, ensure_ascii=False, indent=2), encoding="utf-8")

        logger.debug("Overall summary:\n%s", json.dumps(report, ensure_ascii=False, indent=2))
        render_overall_report(report, self.config.out / "report.html")

        logger.debug("Per-manager summary:\n%s", json.dumps(manager_report, ensure_ascii=False, indent=2))
        render_manager_report(manager_report, self.config.out / "report_by_manager.html")

        logger.info(
            "Reports saved: %s, %s",
            self.config.out / "report.json",
            self.config.out / "report_by_manager.json",
        )


    def sync_to_postgres(self, per_call: List[Dict[str, Any]]) -> None:
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
                if item.get("status") != "processed":
                    continue
                call_id = item.get("meta", {}).get("call_id")
                if not call_id:
                    continue
                pg.upsert_transcript(call_id, self.storage.load_transcript(call_id))
                pg.upsert_analysis(call_id, item.get("analysis", {}))
                synced += 1
            logger.info("Postgres sync complete: %d call(s) upserted", synced)
        finally:
            pg.close()
        
