import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.llm_ollama import OllamaLlm
from adapters.pbx_asterisk import AsteriskPbx
from adapters.pbx_ssh import PbxSshDownloader
from adapters.storage_json import JsonStorage
from api import job_store
from api.schemas import JobStatus, ProcessRequest, SyncRequest
from core.pipeline import Pipeline
from domain.config import CALLS_RAW, load_app_config

logger = logging.getLogger(__name__)


def _day_from_relative_path(relative: Path) -> str | None:
    # Expected shape: YYYY/MM/DD/<filename>
    if len(relative.parts) < 4:
        return None

    year, month, day = relative.parts[0], relative.parts[1], relative.parts[2]
    if (
        len(year) == 4 and year.isdigit()
        and len(month) == 2 and month.isdigit()
        and len(day) == 2 and day.isdigit()
    ):
        return f"{year}/{month}/{day}"
    return None


def _extract_downloaded_days(downloaded_files: list[Path], local_root: Path) -> list[str]:
    days: set[str] = set()
    for local_path in downloaded_files:
        rel = local_path.relative_to(local_root)
        day = _day_from_relative_path(rel)
        if day:
            days.add(day)
    return sorted(days)


def _run_sync_once(req: SyncRequest) -> dict:
    downloader = PbxSshDownloader(
        host=os.environ["PBX_HOST"],
        username=os.getenv("PBX_USER", "asterisk"),
        key_path=os.getenv("PBX_KEY_PATH"),
        known_hosts_path=os.getenv("PBX_KNOWN_HOSTS_PATH"),
        remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
    )
    downloader.connect()
    try:
        new_files = downloader.download_new(CALLS_RAW)
    finally:
        downloader.close()

    downloaded_days = _extract_downloaded_days(new_files, CALLS_RAW)
    downloaded_files = [str(p.relative_to(CALLS_RAW)) for p in new_files]
    return {
        "downloaded": len(new_files),
        "downloaded_days": downloaded_days,
        "downloaded_files": downloaded_files,
    }


def _configure_process_env(req: ProcessRequest) -> None:
    # Inject overrides into environment before loading config
    if req.days is not None:
        os.environ["DAYS"] = req.days
    else:
        os.environ.pop("DAYS", None)

    if req.limit:  # None or 0 both mean "no limit" — let the default apply
        os.environ["PROCESS_LIMIT"] = str(req.limit)
    else:
        os.environ.pop("PROCESS_LIMIT", None)

    os.environ["FORCE_REANALYZE"] = "1" if req.force_reanalyze else "0"
    os.environ["FORCE_RETRANSCRIBE"] = "1" if req.force_retranscribe else "0"


def _run_process_once(req: ProcessRequest) -> dict:
    _configure_process_env(req)
    config = load_app_config()
    storage = JsonStorage(config.out, config.norm, config.trans, config.analysis)
    storage.ensure_dirs()
    pipeline = Pipeline(config=config, storage=storage, audio=FfmpegAudio(), llm=OllamaLlm(config), pbx=AsteriskPbx())
    pipeline.run()
    return {"ok": True}


def run_sync(job_id: str, req: SyncRequest) -> None:
    job_store.update_job(job_id, status=JobStatus.running, started_at=datetime.now(timezone.utc))
    try:
        sync_result = _run_sync_once(req)
        job_store.update_job(
            job_id,
            status=JobStatus.done,
            finished_at=datetime.now(timezone.utc),
            result=sync_result,
        )
    except Exception as exc:
        logger.exception("sync job %s failed", job_id)
        job_store.update_job(
            job_id,
            status=JobStatus.failed,
            finished_at=datetime.now(timezone.utc),
            error=str(exc),
        )


def run_process(job_id: str, req: ProcessRequest) -> None:
    job_store.update_job(job_id, status=JobStatus.running, started_at=datetime.now(timezone.utc))
    try:
        process_result = _run_process_once(req)
        job_store.update_job(
            job_id,
            status=JobStatus.done,
            finished_at=datetime.now(timezone.utc),
            result=process_result,
        )
    except Exception as exc:
        logger.exception("process job %s failed", job_id)
        job_store.update_job(
            job_id,
            status=JobStatus.failed,
            finished_at=datetime.now(timezone.utc),
            error=str(exc),
        )


def run_sync_and_process(job_id: str, req: ProcessRequest) -> None:
    job_store.update_job(job_id, status=JobStatus.running, started_at=datetime.now(timezone.utc))
    try:
        sync_result = _run_sync_once(SyncRequest())
        downloaded_days: list[str] = sync_result.get("downloaded_days", [])

        if downloaded_days:
            scoped_req = ProcessRequest(
                days=",".join(downloaded_days),
                limit=req.limit,
                force_reanalyze=req.force_reanalyze,
                force_retranscribe=req.force_retranscribe,
            )
            process_result = _run_process_once(scoped_req)
            process_result["days"] = downloaded_days
        else:
            process_result = {
                "ok": True,
                "skipped": True,
                "reason": "No new files downloaded",
                "days": [],
            }

        job_store.update_job(
            job_id,
            status=JobStatus.done,
            finished_at=datetime.now(timezone.utc),
            result={
                "sync": sync_result,
                "process": process_result,
            },
        )
    except Exception as exc:
        logger.exception("sync-and-process job %s failed", job_id)
        job_store.update_job(
            job_id,
            status=JobStatus.failed,
            finished_at=datetime.now(timezone.utc),
            error=str(exc),
        )
