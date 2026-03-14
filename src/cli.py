# -*- coding: utf-8 -*-
"""
Local Call Analytics PoC (FreePBX/Asterisk recordings)
- Normalize audio (16k mono)
- Transcribe with faster-whisper (GPU)
- Translate transcript to Ukrainian (UA) via Ollama (optional but enabled by default)
- Analyze calls with Ollama (UA-only JSON schema)
- Aggregate report.json

Folder layout expected:
  ./calls_raw/YYYY/MM/DD/*.wav
Outputs:
  ./out/normalized/*.wav
  ./out/transcripts/*.json
  ./out/analysis/*.json
  ./out/report.json
"""
import os
import sys

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.llm_ollama import OllamaLlm
from adapters.pbx_asterisk import AsteriskPbx
from adapters.storage_json import JsonStorage
from core.pipeline import Pipeline
from domain.config import CALLS_RAW, load_app_config


def sync() -> None:
    from adapters.pbx_ssh import PbxSshDownloader
    host = os.getenv("PBX_HOST")
    if not host:
        print("Error: PBX_HOST environment variable is not set.")
        sys.exit(1)

    downloader = PbxSshDownloader(
      host=host,
      username=os.getenv("PBX_USER", "asterisk"),
      key_path=os.getenv("PBX_KEY_PATH"),
      remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
    )
    downloader.connect()
    new_files = downloader.download_new(CALLS_RAW / "incoming", on_download=print)
    downloader.close()
    print(f"Downloaded {len(new_files)} new file(s).")

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
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    if command == "sync":
        sync()
    elif command == "run":
        main()
    else:
        print(f"Unknown command: {command}. Use 'run' or 'sync'.")
        sys.exit(1)
