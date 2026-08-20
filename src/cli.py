"""
Local Call Analytics PoC (FreePBX/Asterisk recordings)
- Normalize audio (16k mono)
- Transcribe with faster-whisper (GPU)
- Translate transcript to Ukrainian (UA) via Ollama (optional but enabled by default)
- Analyze calls with Ollama (UA-only JSON schema)

Folder layout expected:
  ./calls_raw/YYYY/MM/DD/*.wav
Outputs:
  ./out/normalized/*.wav
  ./out/transcripts/*.json
  ./out/analysis/*.json
"""
import logging
import os
import sys

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.llm_ollama import OllamaLlm
from adapters.pbx_asterisk import AsteriskPbx
from adapters.pbx_ssh import PbxSshDownloader
from adapters.reporting_json import JsonReportingSource
from adapters.reporting_postgres import PostgresReportingSource
from adapters.storage_json import JsonStorage
from adapters.storage_postgres import PostgresStorage
from core.pipeline import Pipeline
from core.snapshot_export import export_snapshot_reports
from adapters.stt_factory import build_stt_adapter
from domain.config import ensure_env_loaded, get_calls_raw, load_app_config
from domain.pbx import load_pbx_config
from logging_config import setup_logging

logger = logging.getLogger(__name__)


def sync() -> None:
    # Explicit PbxConfig with fail-fast on missing PBX_HOST.
    try:
        pbx = load_pbx_config()
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    downloader = PbxSshDownloader(
      host=pbx.host,
      port=pbx.port,
      username=pbx.username,
      password=pbx.password,
      key_path=pbx.key_path,
      known_hosts_path=pbx.known_hosts_path,
      remote_dir=pbx.remote_dir,
    )
    downloader.connect()
    new_files = downloader.download_new(
        get_calls_raw(),
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

    # Provide a Postgres secondary so results are synced to the system of record.
    secondary_storage: PostgresStorage | None = None
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        secondary_storage = PostgresStorage(dsn)
        secondary_storage.ensure_ready()

    pipeline = Pipeline(
      config=config,
      storage=storage,
      audio=FfmpegAudio(),
      llm=OllamaLlm(config),
      pbx=AsteriskPbx(),
      stt=build_stt_adapter(config),
      secondary_storage=secondary_storage,
    )

    # Read the day scope once at entry; core reads it explicitly from here on.
    days = os.getenv("DAYS") or None
    pipeline.run(days=days)


def export_snapshots() -> None:
    config = load_app_config()
    spam_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    dsn = os.getenv("POSTGRES_DSN")
    source = PostgresReportingSource(dsn) if dsn else JsonReportingSource(config.analysis)
    try:
        from adapters.reports_html import HtmlReportRenderer

        result = export_snapshot_reports(
            output_dir=config.out,
            source=source,
            spam_threshold=spam_threshold,
            renderer=HtmlReportRenderer(),
        )
    finally:
        source.close()
    logger.info("Snapshot export completed: %s", result)


if __name__ == "__main__":
    # Load config/.env defaults once at startup (idempotent).
    ensure_env_loaded()
    setup_logging()
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    if command == "sync":
        sync()
    elif command == "export-snapshots":
        export_snapshots()
    elif command == "migrate-storage":
        migrate_storage()
    elif command == "run":
        main()
    else:
        logger.error("Unknown command: %s. Use 'run', 'sync', 'export-snapshots' or 'migrate-storage'.", command)
        sys.exit(1)
