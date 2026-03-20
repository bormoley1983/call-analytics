import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.llm_ollama import OllamaLlm
from adapters.pbx_asterisk import AsteriskPbx
from adapters.pbx_ssh import PbxSshDownloader
from adapters.storage_json import JsonStorage
from adapters.storage_postgres import PostgresStorage
from api import job_store
from api.schemas import JobStatus, ProcessRequest, SyncRequest
from core.pipeline import Pipeline
from domain.config import CALLS_RAW, load_app_config
from ports.storage import StoragePort

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

def _parse_days(days: str | None) -> set[str] | None:
    if not days:
        return None
    parsed = {d.strip().replace("\\", "/") for d in days.split(",") if d.strip()}
    return parsed or None


def _run_sync_once(req: SyncRequest) -> dict:
    downloader = PbxSshDownloader(
        host=os.environ["PBX_HOST"],
        port=int(os.getenv("PBX_PORT", "22")),
        username=os.getenv("PBX_USER", "asterisk"),
        password=os.getenv("PBX_PASSWORD"),
        key_path=os.getenv("PBX_KEY_PATH"),
        known_hosts_path=os.getenv("PBX_KNOWN_HOSTS_PATH"),
        remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
    )
    allowed_days = _parse_days(req.days)
    downloader.connect()
    try:
        new_files = downloader.download_new(CALLS_RAW, allowed_days=allowed_days)
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
    if req.days is not None:
        os.environ["DAYS"] = req.days
    else:
        os.environ.pop("DAYS", None)

    if req.limit is not None:
        os.environ["PROCESS_LIMIT"] = str(req.limit)
    else:
        os.environ.pop("PROCESS_LIMIT", None)

    os.environ["FORCE_REANALYZE"] = "1" if req.force_reanalyze else "0"
    os.environ["FORCE_RETRANSCRIBE"] = "1" if req.force_retranscribe else "0"

    if req.generate_report_snapshots is None:
        os.environ.pop("GENERATE_REPORT_SNAPSHOTS", None)
    else:
        os.environ["GENERATE_REPORT_SNAPSHOTS"] = "1" if req.generate_report_snapshots else "0"


def _run_process_once(req: ProcessRequest) -> dict:
    env_keys = ["DAYS", "PROCESS_LIMIT", "FORCE_REANALYZE", "FORCE_RETRANSCRIBE", "GENERATE_REPORT_SNAPSHOTS"]
    old_env = {k: os.environ.get(k) for k in env_keys}
    try:
        _configure_process_env(req)
        config = load_app_config()
        storage: StoragePort
        if os.getenv("POSTGRES_DSN"):
            logger.info("Postgres storage driver loaded")
            storage = PostgresStorage(os.environ["POSTGRES_DSN"])
        else:
            logger.info("JSON storage driver loaded")
            storage = JsonStorage(config.out, config.norm, config.trans, config.analysis)
        
        storage.ensure_ready()
        try:
            pipeline = Pipeline(
                config=config,
                storage=storage,
                audio=FfmpegAudio(),
                llm=OllamaLlm(config),
                pbx=AsteriskPbx(),
            )
            pipeline.run()
            return {"ok": True}
        finally:
            storage.close()
    finally:
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


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
        process_result = _run_process_once(req)
        process_result["downloaded_days"] = sync_result.get("downloaded_days", [])
        process_result["downloaded"] = sync_result.get("downloaded", 0)

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
