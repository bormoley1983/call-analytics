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

from adapters.storage_json import JsonStorage
from core.pipeline import Pipeline
from domain.config import load_app_config


def main() -> None:
    """Main entry point for call analytics processing."""
    # Load configuration (single source of truth)
    config = load_app_config()
    
    # Setup centralized storage
    storage = JsonStorage(config.out, config.norm, config.trans, config.analysis)
    storage.ensure_dirs()

    Pipeline(config, storage).run()


if __name__ == "__main__":
    main()
