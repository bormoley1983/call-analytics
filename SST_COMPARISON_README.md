# SST Comparison Test Flow

This document defines the immutable STT run-vs-run comparison flow that must be executed before report-level comparison.

## Goal

Compare STT quality directly (transcript-level), isolating STT drift from downstream translation/LLM analysis drift.

## Preconditions

- Python environment is available at `.venv`.
- Input corpus exists under `calls_raw/...`.
- STT run stores are enabled (JSON by default, Postgres when `POSTGRES_DSN` is set).
- Use identical corpus and identical run limit for baseline and candidate.

## Step 1: Run baseline STT replay

Example baseline: faster-whisper with auto language detection.

STT_ENGINE=faster-whisper STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name fw-baseline --purpose benchmark

Optional subset:

STT_ENGINE=faster-whisper STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name fw-baseline --purpose benchmark --limit 200

## Step 2: Run candidate STT replay

Example candidate: canary with auto language detection.

STT_ENGINE=canary STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name canary-candidate --purpose benchmark

Optional subset:

STT_ENGINE=canary STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name canary-candidate --purpose benchmark --limit 200

## Step 3: Collect run IDs

Each replay prints JSON with `run_id`.

If needed (JSON backend), inspect manifests:

find out/stt_runs -name manifest.json | sort

Then open both manifests and copy `run_id` values.

## Step 4: Compare immutable runs

/home/admaccess/call-analytics/.venv/bin/python src/stt_compare.py --baseline-run-id <BASELINE_RUN_ID> --candidate-run-id <CANDIDATE_RUN_ID> --top 30

## Step 5: Read comparison output

Key fields:

- `coverage.common_results`: number of one-to-one comparable calls.
- `coverage.baseline_only`, `coverage.candidate_only`: corpus mismatch indicators.
- `quality.status_mismatch`: decode success/failure drift.
- `quality.mean_text_similarity`: overall transcript closeness.
- `worst_calls`: lowest-similarity calls for manual QA.

## Suggested quality gate before report-level comparison

Proceed to report comparison only if:

- `coverage.common_results` is high enough for confidence (project threshold).
- `quality.status_mismatch` is acceptably low.
- `quality.mean_text_similarity` meets your target for this corpus.
- Worst calls were spot-checked and major regressions are understood.

## Language guidance (UA + RU traffic)

- Prefer `STT_LANGUAGE=auto` for mixed Ukrainian/Russian calls.
- Forcing `STT_LANGUAGE=uk` or `STT_LANGUAGE=ru` applies globally to that run.
- Keep baseline and candidate language settings identical for fair comparison.

## Storage backend note

- If `POSTGRES_DSN` is exported, replay and compare operate on Postgres `stt_runs` / `stt_results`.
- Otherwise they use JSON files under `out/stt_runs`.

## Minimal smoke test command set

STT_ENGINE=faster-whisper STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name smoke-fw --purpose benchmark --limit 10

STT_ENGINE=canary STT_LANGUAGE=auto /home/admaccess/call-analytics/.venv/bin/python src/stt_replay.py --run-name smoke-canary --purpose benchmark --limit 10

/home/admaccess/call-analytics/.venv/bin/python src/stt_compare.py --baseline-run-id <SMOKE_FW_RUN_ID> --candidate-run-id <SMOKE_CANARY_RUN_ID> --top 10
