import json
import os
from pathlib import Path
from typing import Any, Dict, List

from faster_whisper import WhisperModel
from tqdm import tqdm

from adapters.reports_html import render_manager_report, render_overall_report
from core.planner import discover_and_filter_files
from core.reports import aggregate_report, aggregate_report_by_manager
from core.rules import ensure_analysis_schema, sha12
from core.transcription import transcribe
from domain.config import AppConfig
from ports.audio import AudioPort
from ports.llm import LlmPort
from ports.pbx import PbxPort
from ports.storage import StoragePort


class Pipeline:
    def __init__(self, config: AppConfig, storage: StoragePort, audio: AudioPort, llm: LlmPort, pbx: PbxPort):
        self.config = config
        self.storage = storage
        self.audio = audio
        self.llm = llm
        self.pbx = pbx

    def run(self) -> None:
        files_to_process = discover_and_filter_files(self.config)
        if not files_to_process:
            print("No files to process.")
            return
        files_metadata = self.run_transcription_phase(files_to_process)
        per_call = self.run_analysis_phase(files_metadata)
        self.generate_reports(per_call)
    
        print("\n" + "="*80)
        print("✓ PROCESSING COMPLETE")
        print("="*80)

    def run_transcription_phase(self, files: List[Path]) -> List[Dict[str, Any]]:
        """
        Phase 1: Transcription with Whisper (GPU intensive).
        Returns metadata for all files including skipped ones.
        """
        if not files:
            print("No files to process.")
            return []
        
        print("\n" + "="*80)
        print("PHASE 1: TRANSCRIPTION (Whisper)")
        print("="*80)

        model = None
        files_metadata: List[Dict[str, Any]] = []

        for src in tqdm(files, desc="Transcribing"):
            meta = self.pbx.parse_filename(src.name)
            meta["source_file"] = src.name
            meta["source_path"] = str(src)

            # Map to manager
            manager_info = self.config.manager_mapper.find_manager(
                meta.get("src_number", ""),
                meta.get("dst_number", ""),
                meta.get("direction", "unknown")
            )
            meta["manager_name"] = manager_info["name"]
            meta["manager_id"] = manager_info["id"]
            meta["role"] = manager_info.get("role", "unknown")

            # Skip tiny files
            if src.stat().st_size < self.config.min_bytes:
                meta["status"] = "skipped_too_small"
                files_metadata.append(meta)
                continue

            cid = sha12(src.name + str(src.stat().st_size))
            meta["call_id"] = cid

            norm_path = self.config.norm / f"{cid}.wav"
            tr_path = self.storage.transcript_path(cid)
            an_path = self.storage.analysis_path(cid)

            if not norm_path.exists():
                self.audio.normalize(src, norm_path)

            dur = self.audio.duration_seconds(norm_path)
            meta["audio_seconds"] = dur

            if dur < self.config.min_seconds:
                meta["status"] = "skipped_too_short"
                files_metadata.append(meta)
                continue

            # Transcribe
            transcript: Dict[str, Any]
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

            # Ensure UA transcript fields
            changed = False
            try:
                segments = transcript.get("segments", [])
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
            except Exception as e:
                transcript.setdefault("text_uk", transcript.get("text", ""))
                transcript.setdefault("segments_uk", [])
                transcript.setdefault("translation_error", repr(e))
                changed = True

            if self.config.force_retranscribe or changed or (not tr_path.exists()):
                self.storage.save_json(tr_path, transcript)

            meta["status"] = "transcribed"
            meta["tr_path"] = str(tr_path)
            meta["an_path"] = str(an_path)
            files_metadata.append(meta)

        # Free Whisper model from memory
        if model is not None:
            del model
            print("Whisper model released from memory.")
        transcribed_count = len([m for m in files_metadata if m.get('status') == 'transcribed'])
        print(f"\n✓ Transcription complete. Processed {transcribed_count} files.")
        
        return files_metadata


    def run_analysis_phase(self, files_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Phase 2: Analysis with Ollama (different GPU usage pattern).
        Returns per-call results including analysis.
        """
        print("\n" + "="*80)
        print("PHASE 2: ANALYSIS (Ollama)")
        print("="*80)
        
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

        print(f"\n✓ Analysis complete. Processed {len([c for c in per_call if c.get('status') == 'processed'])} calls.")
        
        return per_call


    def generate_reports(self, per_call: List[Dict[str, Any]]) -> None:
        """Generate and save analysis reports."""
        print("\n" + "="*80)
        print("GENERATING REPORTS")
        print("="*80)
        
        # Generate overall report
        report = aggregate_report(per_call, self.config)
        (self.config.out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Generate per-manager report
        manager_report = aggregate_report_by_manager(per_call, self.config)
        (self.config.out / "report_by_manager.json").write_text(json.dumps(manager_report, ensure_ascii=False, indent=2), encoding="utf-8")

        print("\n=== OVERALL SUMMARY ===")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        render_overall_report(report, self.config.out / "report.html")

        print("\n=== PER-MANAGER SUMMARY ===")
        print(json.dumps(manager_report, ensure_ascii=False, indent=2))
        render_manager_report(manager_report, self.config.out / "report_by_manager.html")

        print(f"\n✓ Reports saved:")
        print(f"  - {self.config.out / 'report.json'}")
        print(f"  - {self.config.out / 'report_by_manager.json'}")
        
        if os.getenv("POSTGRES_DSN"):
            self.sync_to_postgres(per_call)


    def sync_to_postgres(self, per_call: List[Dict[str, Any]]) -> None:
        from adapters.storage_postgres import PostgresStorage
        pg = PostgresStorage(os.getenv("POSTGRES_DSN", ""))
        pg.connect()
        pg.sync_per_call(per_call)
        pg.close()
        