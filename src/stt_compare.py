from __future__ import annotations

import argparse
import os
from pathlib import Path

from adapters.stt_runs_json import JsonSttRunStore
from adapters.stt_runs_postgres import PostgresSttRunStore
from core.stt_compare_service import SttCompareService, compare_summary_json
from domain.config import load_app_config
from ports.stt_runs import SttRunStorePort


def _build_run_store(config) -> SttRunStorePort:
    if os.getenv("POSTGRES_DSN"):
        store: PostgresSttRunStore | JsonSttRunStore = PostgresSttRunStore(
            os.environ["POSTGRES_DSN"]
        )
        store.ensure_ready()
        return store
    store = JsonSttRunStore(Path(config.out))
    store.ensure_ready()
    return store


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare immutable STT runs")
    parser.add_argument("--baseline-run-id", required=True)
    parser.add_argument("--candidate-run-id", required=True)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    config = load_app_config()
    run_store = _build_run_store(config)
    try:
        service = SttCompareService(run_store)
        summary = service.compare_runs(
            baseline_run_id=args.baseline_run_id,
            candidate_run_id=args.candidate_run_id,
            top_n=args.top,
        )
        print(compare_summary_json(summary))
    finally:
        run_store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
