from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from core.transcript_fingerprint import transcript_text_sha256
from domain.config import AppConfig
from domain.stt import SttFailure, SttRequest
from domain.stt_runs import SttRunManifest, SttRunPurpose, SttRunResultRecord
from ports.audio import AudioPort
from ports.storage import StoragePort
from ports.stt import SttProcessorPort
from ports.stt_runs import SttRunStorePort


def _sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class SttReplayService:
    def __init__(
        self,
        *,
        config: AppConfig,
        audio: AudioPort,
        stt: SttProcessorPort,
        run_store: SttRunStorePort,
        active_storage: StoragePort,
    ):
        self._config = config
        self._audio = audio
        self._stt = stt
        self._run_store = run_store
        self._active_storage = active_storage

    def _build_manifest(self, *, run_name: str, purpose: str, target_files: list[Path]) -> SttRunManifest:
        now = datetime.now(timezone.utc)
        run_purpose = cast(SttRunPurpose, purpose)
        return SttRunManifest(
            run_id=str(uuid.uuid4()),
            name=run_name,
            purpose=run_purpose,
            provider=self._stt.identity.provider,
            model_id=self._stt.identity.model_id,
            model_revision=self._stt.identity.model_revision,
            config_hash=self._stt.identity.config_hash,
            config_data={
                "stt_engine": self._config.stt_engine,
                "whisper_model": self._config.whisper_model,
                "canary_model_id": self._config.canary_model_id,
            },
            code_revision="unknown",
            dataset_manifest_hash=sha256("\n".join(sorted(str(p) for p in target_files)).encode("utf-8")).hexdigest(),
            hardware_data={"device": getattr(self._config, "canary_device", getattr(self._config, "whisper_device", "cpu"))},
            status="running",
            total_calls=len(target_files),
            started_at=now,
            created_at=now,
        )

    def replay(
        self,
        *,
        target_files: list[Path],
        run_name: str,
        purpose: str = "replay",
        promote_to_active: bool = False,
    ) -> dict[str, Any]:
        self._run_store.ensure_ready()
        manifest = self._build_manifest(run_name=run_name, purpose=purpose, target_files=target_files)
        self._run_store.create_run(manifest)

        completed = 0
        failed = 0
        skipped = 0
        total_audio_seconds = 0.0
        inference_seconds = 0.0

        try:
            for src in target_files:
                st_size = src.stat().st_size
                if st_size < self._config.min_bytes:
                    skipped += 1
                    continue

                call_id = sha256((src.name + str(st_size)).encode("utf-8")).hexdigest()[:12]
                norm_path = self._config.norm / f"{call_id}.wav"
                if not norm_path.exists():
                    self._audio.normalize(src, norm_path)

                audio_seconds = self._audio.duration_seconds(norm_path)
                if audio_seconds < self._config.min_seconds:
                    skipped += 1
                    continue

                total_audio_seconds += audio_seconds
                request = SttRequest(
                    call_id=call_id,
                    audio_path=norm_path,
                    audio_seconds=audio_seconds,
                    audio_sha256=_sha256_file(norm_path),
                    language=self._config.stt_language,
                )

                t0 = time.perf_counter()
                item = next(iter(self._stt.transcribe_many([request])))
                elapsed = time.perf_counter() - t0
                inference_seconds += elapsed

                if isinstance(item, SttFailure):
                    failed += 1
                    self._run_store.upsert_result(
                        SttRunResultRecord(
                            run_id=manifest.run_id,
                            call_id=call_id,
                            audio_sha256=request.audio_sha256,
                            audio_seconds=audio_seconds,
                            status="failed",
                            error_category=item.category,
                            error_detail=item.detail,
                            warnings=[],
                            elapsed_seconds=elapsed,
                            rtf=(elapsed / audio_seconds) if audio_seconds > 0 else 0.0,
                        )
                    )
                    continue

                completed += 1
                canonical_transcript = {
                    "language": item.language,
                    "duration": float(item.duration),
                    "segments": [
                        {"start": float(seg.start), "end": float(seg.end), "text": (seg.text or "").strip()}
                        for seg in item.segments
                        if (seg.text or "").strip()
                    ],
                    "text": (item.raw_text or "").strip(),
                    "_pipeline_stage": "transcribed",
                    "_stt": {
                        "provider": manifest.provider,
                        "model_id": manifest.model_id,
                        "model_revision": manifest.model_revision,
                        "config_hash": manifest.config_hash,
                        "timings": item.timings,
                        "warnings": item.warnings,
                    },
                }
                text_hash = transcript_text_sha256(canonical_transcript)

                self._run_store.upsert_result(
                    SttRunResultRecord(
                        run_id=manifest.run_id,
                        call_id=call_id,
                        audio_sha256=request.audio_sha256,
                        audio_seconds=audio_seconds,
                        status="ok",
                        raw_payload={"text": item.raw_text, "segments": [asdict(s) for s in item.segments]},
                        canonical_payload=canonical_transcript,
                        text_sha256=text_hash,
                        elapsed_seconds=elapsed,
                        rtf=(elapsed / audio_seconds) if audio_seconds > 0 else 0.0,
                        warnings=item.warnings,
                    )
                )

                if promote_to_active:
                    self._active_storage.promote_stt_result(
                        call_id,
                        canonical_transcript,
                        stt_run_id=manifest.run_id,
                        stt_config_hash=manifest.config_hash,
                        source_text_sha256=text_hash,
                    )
                    self._active_storage.mark_analysis_stale_if_text_changed(call_id, text_hash)

            final_status = "completed_with_errors" if failed > 0 else "completed"
            self._run_store.update_run_status(manifest.run_id, final_status)
            self._run_store.update_run_counters(
                manifest.run_id,
                total_calls=len(target_files),
                completed_calls=completed,
                failed_calls=failed,
                skipped_calls=skipped,
            )

            return {
                "run_id": manifest.run_id,
                "status": final_status,
                "counts": {
                    "total": len(target_files),
                    "completed": completed,
                    "failed": failed,
                    "skipped": skipped,
                },
                "timing": {
                    "total_audio_seconds": total_audio_seconds,
                    "inference_seconds": inference_seconds,
                    "avg_rtf": (inference_seconds / total_audio_seconds) if total_audio_seconds > 0 else 0.0,
                },
            }
        except Exception:
            self._run_store.update_run_status(manifest.run_id, "failed")
            raise
        finally:
            self._stt.close()


def replay_summary_json(result: dict[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2)
