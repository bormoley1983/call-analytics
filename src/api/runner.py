import logging
import os
from datetime import datetime, timezone

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

def run_sync(job_id: str, req: SyncRequest) -> None:
    job_store.update_job(job_id, status=JobStatus.running, started_at=datetime.now(timezone.utc))
    try:
        downloader = PbxSshDownloader(
            host=os.environ["PBX_HOST"],
            username=os.getenv("PBX_USER", "asterisk"),
            key_path=os.getenv("PBX_KEY_PATH"),
            remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
        )
        downloader.connect()
        new_files = downloader.download_new(CALLS_RAW / "incoming")
        downloader.close()
        job_store.update_job(
            job_id,
            status=JobStatus.done,
            finished_at=datetime.now(timezone.utc),
            result={"downloaded": len(new_files)},
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
    # Inject overrides into environment before loading config
    if req.days:
        os.environ["DAYS"] = req.days
    if req.limit:
        os.environ["PROCESS_LIMIT"] = str(req.limit)
    if req.force_reanalyze:
        os.environ["FORCE_REANALYZE"] = "1"
    if req.force_retranscribe:
        os.environ["FORCE_RETRANSCRIBE"] = "1"

    job_store.update_job(job_id, status=JobStatus.running, started_at=datetime.now(timezone.utc))
    try:
        config = load_app_config()
        storage = JsonStorage(config.out, config.norm, config.trans, config.analysis)
        storage.ensure_dirs()
        pipeline = Pipeline(config=config, storage=storage, audio=FfmpegAudio(), llm=OllamaLlm(config), pbx=AsteriskPbx())
        pipeline.run()
        job_store.update_job(
            job_id,
            status=JobStatus.done,
            finished_at=datetime.now(timezone.utc),
            result={"ok": True},
        )
    except Exception as exc:
        logger.exception("process job %s failed", job_id)
        job_store.update_job(
            job_id,
            status=JobStatus.failed,
            finished_at=datetime.now(timezone.utc),
            error=str(exc),
        )