from __future__ import annotations

import difflib
import json
from statistics import mean
from typing import Any

from ports.stt_runs import SttRunStorePort


def _text_from_payload(payload: dict[str, Any]) -> str:
    text = payload.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    segments = payload.get("segments")
    if isinstance(segments, list):
        parts: list[str] = []
        for seg in segments:
            if isinstance(seg, dict):
                t = str(seg.get("text") or "").strip()
                if t:
                    parts.append(t)
        return "\n".join(parts).strip()
    return ""


def _similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    return difflib.SequenceMatcher(a=a, b=b).ratio()


class SttCompareService:
    def __init__(self, run_store: SttRunStorePort):
        self._run_store = run_store

    def compare_runs(self, baseline_run_id: str, candidate_run_id: str, *, top_n: int = 20) -> dict[str, Any]:
        baseline_run = self._run_store.load_run(baseline_run_id)
        candidate_run = self._run_store.load_run(candidate_run_id)

        baseline_results = {r.call_id: r for r in self._run_store.iter_results(baseline_run_id)}
        candidate_results = {r.call_id: r for r in self._run_store.iter_results(candidate_run_id)}

        baseline_ids = set(baseline_results.keys())
        candidate_ids = set(candidate_results.keys())
        common_ids = sorted(baseline_ids & candidate_ids)

        compared: list[dict[str, Any]] = []
        status_mismatch = 0

        for call_id in common_ids:
            b = baseline_results[call_id]
            c = candidate_results[call_id]
            if b.status != c.status:
                status_mismatch += 1

            b_text = _text_from_payload(b.canonical_payload)
            c_text = _text_from_payload(c.canonical_payload)
            sim = _similarity(b_text, c_text)
            compared.append(
                {
                    "call_id": call_id,
                    "baseline_status": b.status,
                    "candidate_status": c.status,
                    "text_similarity": sim,
                    "baseline_text_len": len(b_text),
                    "candidate_text_len": len(c_text),
                    "baseline_rtf": b.rtf,
                    "candidate_rtf": c.rtf,
                    "baseline_excerpt": b_text[:220],
                    "candidate_excerpt": c_text[:220],
                }
            )

        sim_values = [x["text_similarity"] for x in compared]
        worst = sorted(compared, key=lambda x: x["text_similarity"])[: max(1, top_n)]

        return {
            "baseline": {
                "run_id": baseline_run_id,
                "provider": baseline_run.provider,
                "model_id": baseline_run.model_id,
                "config_hash": baseline_run.config_hash,
            },
            "candidate": {
                "run_id": candidate_run_id,
                "provider": candidate_run.provider,
                "model_id": candidate_run.model_id,
                "config_hash": candidate_run.config_hash,
            },
            "coverage": {
                "baseline_results": len(baseline_ids),
                "candidate_results": len(candidate_ids),
                "common_results": len(common_ids),
                "baseline_only": len(baseline_ids - candidate_ids),
                "candidate_only": len(candidate_ids - baseline_ids),
            },
            "quality": {
                "status_mismatch": status_mismatch,
                "mean_text_similarity": mean(sim_values) if sim_values else 0.0,
                "min_text_similarity": min(sim_values) if sim_values else 0.0,
                "max_text_similarity": max(sim_values) if sim_values else 0.0,
            },
            "worst_calls": worst,
        }


def compare_summary_json(summary: dict[str, Any]) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2)
