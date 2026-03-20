from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class JsonStorage:
    def __init__(self, out: Path, norm: Path, trans: Path, analysis: Path):
        self.out = out
        self.norm = norm
        self.trans = trans
        self.analysis = analysis

    # --- lifecycle ---

    def ensure_ready(self) -> None:
        for p in [self.out, self.norm, self.trans, self.analysis]:
            p.mkdir(parents=True, exist_ok=True)

    def ensure_dirs(self) -> None:
        self.ensure_ready()

    def close(self) -> None:
        pass      

    # --- internal path helpers (also used by planner + migrate_storage) ---

    def transcript_path(self, call_id: str) -> Path:
        return self.trans / f"{call_id}.json"

    def analysis_path(self, call_id: str) -> Path:
        return self.analysis / f"{call_id}.json"
    
    # --- StoragePort interface ---

    def transcript_exists(self, call_id: str) -> bool:
        return self.transcript_path(call_id).exists()

    def analysis_exists(self, call_id: str) -> bool:
        return self.analysis_path(call_id).exists()

    def load_transcript(self, call_id: str) -> Dict[str, Any]:
        return json.loads(self.transcript_path(call_id).read_text(encoding="utf-8"))

    def load_analysis(self, call_id: str) -> Dict[str, Any]:
        return json.loads(self.analysis_path(call_id).read_text(encoding="utf-8"))

    def save_transcript(self, call_id: str, data: Dict[str, Any]) -> None:
        self.transcript_path(call_id).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def save_analysis(self, call_id: str, data: Dict[str, Any]) -> None:
        self.analysis_path(call_id).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
