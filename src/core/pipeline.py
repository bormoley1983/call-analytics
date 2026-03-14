import json
import logging
import os
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


class Pipeline:
    def __init__(self, config: AppConfig, storage: StoragePort, audio: AudioPort, llm: LlmPort, pbx: PbxPort):
        self.config = config
        self.storage = storage
        self.audio = audio
        self.llm = llm
        self.pbx = pbx

    def run(self) -> None:
        all_files = discover_and_filter_files(self.config)
        if not all_files:
            logger.info("No files to process.")
            return
        
        needs_pipeline, analysis_only = categorize_files(all_files, self.config)
        logger.info(
            "%d file(s) need pipeline, %d file(s) need analysis only",
            len(needs_pipeline), len(analysis_only),
        )

        files_metadata = self.run_transcription_phase(needs_pipeline)

        # Inject analysis-only files directly — skip Whisper and translation entirely
        for src in analysis_only:
            meta = self._build_meta(src)
            tr_path = Path(meta["tr_path"])
            transcript = self.storage.load_json(tr_path)
            meta["audio_seconds"] = transcript.get("call_meta", {}).get("audio_seconds") or \
                                    self.audio.duration_seconds(self.config.norm / f"{meta['call_id']}.wav")
            meta["status"] = "transcribed"
            meta["stage"] = "translated"
            files_metadata.append(meta)

        per_call = self.run_analysis_phase(files_metadata)
        self.generate_reports(per_call)
        logger.info("Processing complete.")

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

        if src.stat().st_size >= self.config.min_bytes:
            cid = sha12(src.name + str(src.stat().st_size))
            meta["call_id"] = cid
            meta["tr_path"] = str(self.storage.transcript_path(cid))
            meta["an_path"] = str(self.storage.analysis_path(cid))

        return meta

    def run_transcription_phase(self, files: List[Path]) -> List[Dict[str, Any]]:
        """
        Phase 1: Transcription with Whisper (GPU intensive).
        Returns metadata for all files including skipped ones.
        """
        if not files:
            logger.info("No files to process.")
            return []

        logger.info("Phase 1: Transcription (Whisper)")

        model = None
        files_metadata: List[Dict[str, Any]] = []

        for src in tqdm(files, desc="Transcribing"):
            meta = self._build_meta(src)
            if "call_id" not in meta:
                meta["status"] = "skipped_too_small"
                files_metadata.append(meta)
                continue

            dur = self.audio.duration_seconds(src)
            meta["audio_seconds"] = dur

            if dur < self.config.min_seconds:
                meta["status"] = "skipped_too_short"
                files_metadata.append(meta)
                continue
            
            norm_path = self.config.norm / f"{meta['call_id']}.wav"
            tr_path = Path(meta["tr_path"])

            if not norm_path.exists():
                self.audio.normalize(src, norm_path)

            # Transcribe
            transcript: Dict[str, Any]
            newly_transcribed = False
            if (not self.config.force_retranscribe) and tr_path.exists():
                transcript = self.storage.load_json(tr_path)
            else:
                if model is None:
                    model = WhisperModel(
                       self.config.whisper_model,
                        device=self.config.whisper_device,
                        compute_type=self.config.whisper_compute_type
                    )
                transcript = transcribe(model, norm_path, self.config)
                newly_transcribed = True

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
                self.storage.save_json(tr_path, transcript)

            # Ensure UA transcript fields
            completed_stage = transcript.get("_pipeline_stage", "transcribed")
            need_translate = (
                completed_stage != "translated"
                or self.config.force_retranscribe
                or self.config.force_translate_uk
            )

            if need_translate:
                segments = transcript.get("segments", [])
                try:
                    translated = self.llm.translate_segments_to_uk(segments)
                    if translated:
                        transcript["text_uk"] = "\n".join(translated)
                        transcript["segments_uk"] = [
                            {"start": seg["start"], "end": seg["end"], "text": uk}
                            for seg, uk in zip(segments, translated)
                        ]
                    else:
                        transcript.setdefault("text_uk", transcript.get("text", ""))
                        transcript.setdefault("segments_uk", segments)
                    transcript["_pipeline_stage"] = "translated"

                    self.storage.save_json(tr_path, transcript)
                except Exception as e:
                    transcript.setdefault("text_uk", transcript.get("text", ""))
                    transcript.setdefault("segments_uk", [])
                    transcript.setdefault("translation_error", repr(e))



            meta["stage"] = transcript.get("_pipeline_stage", "transcribed")
            meta["status"] = "transcribed"
            files_metadata.append(meta)

        # Free Whisper model from memory
        if model is not None:
            del model
            logger.info("Whisper model released from memory.")
        transcribed_count = len([m for m in files_metadata if m.get('status') == 'transcribed'])
        logger.info("Transcription complete. Processed %d file(s).", transcribed_count)
        
        return files_metadata


    def run_analysis_phase(self, files_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Phase 2: Analysis with Ollama (different GPU usage pattern).
        Returns per-call results including analysis.
        """
        logger.info("Phase 2: Analysis (Ollama)")

        per_call: List[Dict[str, Any]] = []

        for meta in tqdm([m for m in files_metadata if m.get("status") == "transcribed"], desc="Analyzing"):
            tr_path = Path(meta["tr_path"])
            an_path = Path(meta["an_path"])
            
            # Load transcript
            transcript = self.storage.load_json(tr_path)
            
            # Analyze
            analysis: Dict[str, Any]
            if (not self.config.force_reanalyze) and an_path.exists():
                analysis = self.storage.load_json(an_path)
                analysis = ensure_analysis_schema(analysis, meta)
            else:
                text_uk = (transcript.get("text_uk") or transcript.get("text") or "").strip()
                try:
                    analysis = self.llm.analyze(meta, text_uk)
                except Exception as e:
                    analysis = ensure_analysis_schema({}, meta)
                    analysis["effective_call"] = False
                    analysis["spam_probability"] = 1.0
                    analysis["intent"] = "інше"
                    analysis["outcome"] = "невідомо"
                    analysis["summary"] = "Не вдалося отримати коректний JSON-аналіз від моделі."
                    analysis["analysis_error"] = repr(e)

            # Add manager info to analysis
            analysis["manager_name"] = meta["manager_name"]
            analysis["manager_id"] = meta["manager_id"]
            analysis["role"] = meta["role"]
            analysis["call_meta"] = {
                "direction": meta.get("direction"),
                "src_number": meta.get("src_number"),
                "dst_number": meta.get("dst_number"),
                "date": meta.get("date"),
                "time": meta.get("time"),
                "audio_seconds": meta.get("audio_seconds"),
            }

            # Always save normalized analysis
            self.storage.save_json(an_path, analysis)
            per_call.append({"meta": meta, "analysis": analysis, "status": "processed"})
        
        # Add skipped files to per_call for report
        for meta in files_metadata:
            if meta.get("status") in ("skipped_too_small", "skipped_too_short"):
                per_call.append({"meta": meta, "status": meta["status"]})

        logger.info(
            "Analysis complete. Processed %d call(s).",
            len([c for c in per_call if c.get('status') == 'processed']),
        )

        return per_call


    def generate_reports(self, per_call: List[Dict[str, Any]]) -> None:
        """Generate and save analysis reports."""
        logger.info("Generating reports")
        
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
        if os.getenv("POSTGRES_DSN"):
            self.sync_to_postgres(per_call)


    def sync_to_postgres(self, per_call: List[Dict[str, Any]]) -> None:
        pg = PostgresStorage(os.getenv("POSTGRES_DSN", ""))
        pg.connect()
        pg.sync_per_call(per_call)
        pg.close()
        