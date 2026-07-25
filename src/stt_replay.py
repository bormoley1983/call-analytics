from __future__ import annotations

import argparse
import os
from pathlib import Path

from adapters.audio_ffmpeg import FfmpegAudio
from adapters.storage_json import JsonStorage
from adapters.storage_postgres import PostgresStorage
from adapters.stt_runs_json import JsonSttRunStore
from adapters.stt_runs_postgres import PostgresSttRunStore
from core.planner import discover_all_wav_files
from core.stt_factory import build_stt_adapter
from core.stt_replay_service import SttReplayService, replay_summary_json
from domain.config import load_app_config
from logging_config import setup_logging
from ports.storage import StoragePort
from ports.stt_runs import SttRunStorePort


def _build_storage(config) -> StoragePort:
    if os.getenv("POSTGRES_DSN"):
        pg = PostgresStorage(os.environ["POSTGRES_DSN"])
        pg.ensure_ready()
        return pg
    js = JsonStorage(config.out, config.norm, config.trans, config.analysis)
    js.ensure_ready()
    return js


def _build_run_store(config) -> SttRunStorePort:
    if os.getenv("POSTGRES_DSN"):
        pg = PostgresSttRunStore(os.environ["POSTGRES_DSN"])
        pg.ensure_ready()
        return pg
    js = JsonSttRunStore(config.out)
    js.ensure_ready()
    return js


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay corpus through selected STT engine and persist immutable results")
    parser.add_argument("--run-name", default="stt-replay", help="Logical run name")
    parser.add_argument("--purpose", default="replay", choices=["production", "replay", "benchmark", "legacy_import"])
    parser.add_argument("--promote", action="store_true", help="Promote replay result to active transcript and invalidate stale analyses")
    parser.add_argument("--limit", type=int, default=0, help="Optional max files to replay (0 means all)")
    return parser


def main() -> int:
    setup_logging()
    args = build_parser().parse_args()

    config = load_app_config()
    storage = _build_storage(config)
    run_store = _build_run_store(config)
    stt = build_stt_adapter(config)

    try:
        stat_cache: dict[Path, os.stat_result] = {}
        files = discover_all_wav_files(config, stat_cache)
        if args.limit > 0:
            files = files[: args.limit]

        service = SttReplayService(
            config=config,
            audio=FfmpegAudio(),
            stt=stt,
            run_store=run_store,
            active_storage=storage,
        )
        result = service.replay(
            target_files=files,
            run_name=args.run_name,
            purpose=args.purpose,
            promote_to_active=args.promote,
        )
        print(replay_summary_json(result))
        return 0
    finally:
        storage.close()
        run_store.close()


if __name__ == "__main__":
    raise SystemExit(main())
