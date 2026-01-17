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

    def ensure_dirs(self) -> None:
        for p in [self.out, self.norm, self.trans, self.analysis]:
            p.mkdir(parents=True, exist_ok=True)         

    def transcript_path(self, call_id: str) -> Path:
        return self.trans / f"{call_id}.json"

    def analysis_path(self, call_id: str) -> Path:
        return self.analysis / f"{call_id}.json"

    def load_json(self, path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def save_json(self, path: Path, data: Dict[str, Any]) -> None:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
