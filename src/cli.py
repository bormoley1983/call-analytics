# -*- coding: utf-8 -*-
"""
Local Call Analytics PoC (FreePBX/Asterisk recordings)
- Normalize audio (16k mono)
- Transcribe with faster-whisper (GPU)
- Translate transcript to Ukrainian (UA) via Ollama (optional but enabled by default)
- Analyze calls with Ollama (UA-only JSON schema)
- Optionally aggregate report snapshots (when `GENERATE_REPORT_SNAPSHOTS=1`)

Folder layout expected:
  ./calls_raw/YYYY/MM/DD/*.wav
Outputs:
  ./out/normalized/*.wav
  ./out/transcripts/*.json
  ./out/analysis/*.json
  ./out/report.json (optional snapshot)
"""
import logging
import os
import sys

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.llm_ollama import OllamaLlm
from adapters.pbx_asterisk import AsteriskPbx
from adapters.pbx_ssh import PbxSshDownloader
from adapters.storage_json import JsonStorage
from core.pipeline import Pipeline
from domain.config import CALLS_RAW, load_app_config
from logging_config import setup_logging

logger = logging.getLogger(__name__)


def sync() -> None:
    host = os.getenv("PBX_HOST")
    if not host:
        logger.error("PBX_HOST environment variable is not set.")
        sys.exit(1)

    downloader = PbxSshDownloader(
      host=host,
      username=os.getenv("PBX_USER", "asterisk"),
      key_path=os.getenv("PBX_KEY_PATH"),
      remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
    )
    downloader.connect()
    new_files = downloader.download_new(
        CALLS_RAW,
        on_download=lambda f: logger.info("Downloaded: %s", f),
    )
    downloader.close()
    logger.info("Downloaded %d new file(s).", len(new_files))


def migrate_storage() -> None:
    # Delegate to dedicated migration module.
    from migrate_storage import main as migrate_main

    # Preserve sub-args: cli.py migrate-storage --source ... --target ...
    sys.argv = [sys.argv[0], *sys.argv[2:]]
    raise SystemExit(migrate_main())


def main() -> None:
    """Main entry point for call analytics processing."""
    # Load configuration (single source of truth)
    config = load_app_config()
    
    # Setup centralized storage
    storage = JsonStorage(config.out, config.norm, config.trans, config.analysis)
    storage.ensure_dirs()

    pipeline = Pipeline(
      config=config,
      storage=storage,
      audio=FfmpegAudio(),
      llm=OllamaLlm(config),
      pbx=AsteriskPbx(),
    )

    pipeline.run()


if __name__ == "__main__":
    setup_logging()
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    if command == "sync":
        sync()
    elif command == "migrate-storage":
        migrate_storage()
    elif command == "run":
        main()
    else:
        logger.error("Unknown command: %s. Use 'run', 'sync' or 'migrate-storage'.", command)
        sys.exit(1)
