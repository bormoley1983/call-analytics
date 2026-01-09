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
import json
import re
import time
import subprocess
import hashlib
import yaml
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
from tqdm import tqdm
from faster_whisper import WhisperModel

# ----------------------------
# Paths
# ----------------------------
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
CALLS_RAW = ROOT / "calls_raw"
OUT = ROOT / "out"
NORM = OUT / "normalized"
TRANS = OUT / "transcripts"
ANALYSIS = OUT / "analysis"
CONFIG_DIR = ROOT / "config"
MANAGERS_CONFIG = CONFIG_DIR / "managers.yaml"
BRANDS_CONFIG = CONFIG_DIR / "brands.yaml"

# ----------------------------
# Env config
# ----------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:latest")

WHISPER_MODEL = os.getenv("WHISPER_MODEL", "medium")          # small|medium|large-v3
DEVICE = os.getenv("WHISPER_DEVICE", "cuda")                  # cuda|cpu
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "float16")   # cuda: float16 ; cpu: int8

MIN_BYTES = int(os.getenv("MIN_BYTES", "20000"))              # skip tiny files
MIN_SECONDS = float(os.getenv("MIN_SECONDS", "1.0"))          # skip very short audio

# Processing limit (default 30 files)
PROCESS_LIMIT = int(os.getenv("PROCESS_LIMIT", "30"))

# Processing limit (default 30 files)
#DAYS = int(os.getenv("DAYS", "2026/01/01,2026/01/02,2026/01/03,2026/01/04,2026/01/05"))

# Force regeneration controls
FORCE_REANALYZE = os.getenv("FORCE_REANALYZE", "0") == "0"
FORCE_RETRANSCRIBE = os.getenv("FORCE_RETRANSCRIBE", "0") == "0"
FORCE_TRANSLATE_UK = os.getenv("FORCE_TRANSLATE_UK", "1") == "1"  # default ON (you want UA)

# Translation batching limits (to keep prompts small)
MAX_SEGMENTS_TRANSLATE = int(os.getenv("MAX_SEGMENTS_TRANSLATE", "60"))
MAX_CHARS_TRANSLATE = int(os.getenv("MAX_CHARS_TRANSLATE", "12000"))
MAX_CHARS_ANALYZE = int(os.getenv("MAX_CHARS_ANALYZE", "9000"))

# ----------------------------
# Helpers
# ----------------------------
def ensure_dirs() -> None:
    for p in [OUT, NORM, TRANS, ANALYSIS]:
        p.mkdir(parents=True, exist_ok=True)

def sha12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]

def discover_all_wav_files() -> List[Path]:
    """
    Recursively discover all .wav files under CALLS_RAW directory.
    Returns them sorted by modification time (oldest first) for consistent processing.
    """
    if not CALLS_RAW.exists():
        return []
    
    all_files = list(CALLS_RAW.rglob("*.wav"))
    # Sort by modification time (oldest first)
    all_files.sort(key=lambda p: p.stat().st_mtime)
    return all_files

def discover_wav_files_from_specified_dirs() -> List[Path]:
    """
    Discover WAV files from specific date directories.
    Expects DAYS env var: "2026/01/01,2026/01/02,..."
    Returns sorted list of .wav files from those directories.
    """
    days_env = os.getenv("DAYS", "")
    
    if not days_env.strip():
        print("DAYS env var not set or empty. Returning empty list.")
        return []
																 
    day_list = [d.strip().replace("\\", "/") for d in days_env.split(",") if d.strip()]
    all_files: List[Path] = []

    for d in day_list:
        day_path = CALLS_RAW / d
        if day_path.exists():
            all_files.extend(day_path.glob("*.wav"))

    all_files = sorted(all_files)
																	
    if not all_files:
        print("No WAV files found. Checked day folders under:", CALLS_RAW)
        for d in day_list:
            print("  ", (CALLS_RAW / d))
        return

    return all_files

def filter_unprocessed_files(files: List[Path]) -> List[Path]:
    """
    Filter out files that have already been processed.
    A file is considered processed if both transcript and analysis exist.
    """
    unprocessed = []
    for src in files:
        cid = sha12(src.name + str(src.stat().st_size))
        tr_path = TRANS / f"{cid}.json"
        an_path = ANALYSIS / f"{cid}.json"
        
        # If forcing re-processing, include all
        if FORCE_RETRANSCRIBE or FORCE_REANALYZE:
            unprocessed.append(src)
        # Otherwise only include if not fully processed
        elif not (tr_path.exists() and an_path.exists()):
            unprocessed.append(src)
    
    return unprocessed

def parse_freepbx_filename(name: str) -> Dict[str, Any]:
    # Expected: dir-dst-src-YYYYMMDD-HHMMSS-uniqueid.wav
    base = name.rsplit(".", 1)[0] if "." in name else name
    parts = base.split("-")
    meta: Dict[str, Any] = {"raw_name": name}

    if len(parts) < 6:
        meta["direction"] = "unknown"
        return meta

    dir_tag, dst, src, yyyymmdd, hhmmss = parts[0], parts[1], parts[2], parts[3], parts[4]
    uniqueid = "-".join(parts[5:])

    direction = "incoming" if dir_tag == "in" else "outgoing" if dir_tag == "out" else "unknown"

    meta.update(
        {
            "direction": direction,
            "dst_number": dst,
            "src_number": src,
            "date": yyyymmdd,
            "time": hhmmss,
            "asterisk_uniqueid": uniqueid,
        }
    )
    return meta

def ffprobe_duration_seconds(path: Path) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path)
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return 0.0
    try:
        return float(p.stdout.strip())
    except Exception:
        return 0.0

def normalize_audio(src: Path, dst: Path) -> None:
    # Convert to 16kHz mono wav (better for Whisper, even if source is 8kHz)
    cmd = ["ffmpeg", "-y", "-i", str(src), "-ac", "1", "-ar", "16000", "-vn", str(dst)]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def transcribe(model: WhisperModel, wav_path: Path) -> Dict[str, Any]:
    segments, info = model.transcribe(
        str(wav_path),
        vad_filter=True,
        beam_size=5,
        word_timestamps=False,
    )
    seg_list: List[Dict[str, Any]] = []
    full_text: List[str] = []
    for s in segments:
        t = (s.text or "").strip()
        if not t:
            continue
        seg_list.append({"start": float(s.start), "end": float(s.end), "text": t})
        full_text.append(t)

    return {
        "language": info.language,
        "duration": float(info.duration),
        "segments": seg_list,
        "text": "\n".join(full_text).strip(),
    }

def _ollama_generate(prompt: str, temperature: float = 0.2, timeout: int = 600, retries: int = 4, force_json: bool = False) -> str:
    last_err: Exception | None = None
    last_err: Exception | None = None

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }
    if force_json:
        payload["format"] = "json"
        payload["options"]["temperature"] = 0.0

    for attempt in range(retries):
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json=payload,
                timeout=timeout,
            )
            r.raise_for_status()
            return (r.json().get("response", "") or "").strip()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Ollama request failed after {retries} retries: {last_err!r}")

def _extract_json_object(raw: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        raise ValueError(f"Ollama did not return JSON. Raw head: {raw[:250]}")
    return json.loads(m.group(0))

def translate_segments_to_uk(segments: List[Dict[str, Any]]) -> List[str] | None:
    """
    Translate segment texts to Ukrainian in a single call.
    Returns list of translated strings in same order, or None if too large.
    """
    texts = [(seg.get("text") or "").strip() for seg in segments]
    texts = [t for t in texts if t]

    if not texts:
        return []

    joined = "\n".join(f"{i+1}. {t}" for i, t in enumerate(texts))
    if len(texts) > MAX_SEGMENTS_TRANSLATE or len(joined) > MAX_CHARS_TRANSLATE:
        return None

    prompt = f"""
Translate the numbered lines below to Ukrainian (Cyrillic).
Rules:
- Keep the same number of lines.
- Preserve numbers, product codes, names, phone numbers.
- Return ONLY valid JSON array of strings in Ukrainian, same order as input.
No markdown, no extra text.

Input lines:
{joined}
""".strip()

    raw = _ollama_generate(prompt, temperature=0.0, timeout=600)
    arr = None
    try:
        arr = json.loads(raw)
    except Exception:
        # try to salvage if model wrapped it
        m = re.search(r"\[.*\]", raw, flags=re.S)
        if m:
            arr = json.loads(m.group(0))

    if not isinstance(arr, list) or not all(isinstance(x, str) for x in arr):
        raise ValueError(f"Translate did not return JSON array of strings. Raw head: {raw[:250]}")

    if len(arr) != len(texts):
        raise ValueError(f"Translate lines count mismatch: got {len(arr)}, expected {len(texts)}")

    return arr

def translate_text_to_uk(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    prompt = f"""
Translate the text below to Ukrainian (Cyrillic).
Preserve numbers, product codes, names, phone numbers.
Return ONLY the translated text. No quotes, no commentary.

Text:
{text}
""".strip()
    return _ollama_generate(prompt, temperature=0.0, timeout=600)

def ensure_transcript_uk(transcript: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """
    Ensures transcript contains:
      - text_uk
      - segments_uk (if feasible)
    Returns (transcript, changed_flag)
    """
    changed = False
    txt = (transcript.get("text") or "").strip()
    segs = transcript.get("segments") or []

    # If already present and we are not forcing, keep.
    if not FORCE_TRANSLATE_UK and "text_uk" in transcript:
        return transcript, False

    # Prefer segment translation (keeps timestamps)
    segments_uk = None
    try:
        if isinstance(segs, list) and segs:
            translated_lines = translate_segments_to_uk(segs)
            if translated_lines is not None:
                segments_uk = []
                idx = 0
                for seg in segs:
                    t = (seg.get("text") or "").strip()
                    if not t:
                        continue
                    segments_uk.append(
                        {
                            "start": float(seg.get("start", 0.0)),
                            "end": float(seg.get("end", 0.0)),
                            "text": translated_lines[idx],
                        }
                    )
                    idx += 1
    except Exception:
        segments_uk = None  # fallback to full text translation

    if segments_uk is not None and len(segments_uk) > 0:
        transcript["segments_uk"] = segments_uk
        transcript["text_uk"] = "\n".join([s["text"] for s in segments_uk]).strip()
        changed = True
        return transcript, changed

    # Fallback: translate the whole text
    transcript["text_uk"] = translate_text_to_uk(txt)
    changed = True
    return transcript, changed

def ensure_analysis_schema(analysis: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    a = analysis or {}

    # Defaults
    a.setdefault("spam_probability", 0.0)
    a.setdefault("effective_call", True)
    a.setdefault("intent", "інше")
    a.setdefault("direction", meta.get("direction", "unknown"))
    a.setdefault("outcome", "невідомо")
    a.setdefault("key_questions", [])
    a.setdefault("objections", [])
    a.setdefault("summary", "")
    a.setdefault("action_items", [])
    a.setdefault("suggested_script", [])

    # Coerce types
    try:
        a["spam_probability"] = float(a.get("spam_probability", 0.0))
    except Exception:
        a["spam_probability"] = 0.0
    a["spam_probability"] = max(0.0, min(1.0, a["spam_probability"]))

    if not isinstance(a.get("key_questions"), list):
        a["key_questions"] = []
    if not isinstance(a.get("objections"), list):
        a["objections"] = []
    if not isinstance(a.get("action_items"), list):
        a["action_items"] = []
    if not isinstance(a.get("suggested_script"), list):
        a["suggested_script"] = []

    # Normalize direction
    if a.get("direction") not in ("incoming", "outgoing", "unknown"):
        a["direction"] = meta.get("direction", "unknown")

    return a

def ollama_analyze(call_meta: Dict[str, Any], transcript_text_uk: str) -> Dict[str, Any]:
    t = (transcript_text_uk or "").strip()
    if len(t) > MAX_CHARS_ANALYZE:
        t = t[:MAX_CHARS_ANALYZE] + "\n..."

    prompt = f"""
Ти аналізуєш телефонні дзвінки для e-commerce магазину.

ВАЖЛИВО:
- ПОВЕРТАЙ ЛИШЕ валідний JSON. Без markdown. Без пояснень.
- УСІ текстові поля мають бути УКРАЇНСЬКОЮ (кирилиця), навіть якщо дзвінок був російською.
- Якщо транскрипт порожній/шум/повтори — все одно поверни JSON за схемою (spam_probability=1.0, effective_call=false).

Використовуй фіксовану таксономію:
intent: один із ["доставка","ціна","наявність","оплата","гарантія","повернення","скарга","консультація","інше"]
outcome: один із ["продаж","потрібен_фоллоуап","лише_інфо","втрачено","невідомо"]

Схема:
{{
  "spam_probability": number,
  "effective_call": boolean,
  "intent": string,
  "direction": "{call_meta.get("direction","unknown")}",
  "outcome": string,
  "key_questions": [string],
  "objections": [string],
  "summary": string,
  "action_items": [string],
  "suggested_script": [string]
}}

Метадані:
{json.dumps(call_meta, ensure_ascii=False)}

Транскрипт (українською):
{transcript_text_uk}
""".strip()

    raw = _ollama_generate(prompt, timeout=600, force_json=True)
    
    # With format=json we expect full JSON object
    try:
        analysis = json.loads(raw)
    except Exception:
        # salvage attempt (should be rare)
        analysis = _extract_json_object(raw)

    return ensure_analysis_schema(analysis, call_meta)

def aggregate_report(per_call: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(per_call)
    processed = [c for c in per_call if c.get("status") == "processed"]
    skipped = [c for c in per_call if c.get("status") != "processed"]

    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except Exception:
            return default

    spam = sum(1 for c in processed if num((c.get("analysis") or {}).get("spam_probability", 0.0)) >= 0.7)
							
    effective = sum(1 for c in processed if (c.get("analysis") or {}).get("effective_call") is True)

    by_dir = {"incoming": 0, "outgoing": 0, "unknown": 0}
    intents: Dict[str, int] = {}
    questions: Dict[str, int] = {}

    for c in processed:
        a = c.get("analysis") or {}
        d = a.get("direction", "unknown")
        by_dir[d] = by_dir.get(d, 0) + 1

        intent = a.get("intent", "інше")
        intents[intent] = intents.get(intent, 0) + 1

        for q in a.get("key_questions", []) or []:
            qq = (q or "").strip()
            if qq:
                questions[qq] = questions.get(qq, 0) + 1

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_files_seen": total,
        "processed_calls": len(processed),
        "skipped_files": len(skipped),
        "skipped_breakdown": {
            "too_small_or_empty": sum(1 for c in skipped if c.get("status") == "skipped_too_small"),
            "too_short_audio": sum(1 for c in skipped if c.get("status") == "skipped_too_short"),
        },
        "spam_calls_estimated": spam,
        "effective_calls_estimated": effective,
        "by_direction": by_dir,
        "top_intents": sorted(intents.items(), key=lambda kv: kv[1], reverse=True)[:10],
        "top_questions": sorted(questions.items(), key=lambda kv: kv[1], reverse=True)[:15],
    }

def load_brand_corrections() -> Tuple[Dict[str, str], str]:
    """
    Load brand name corrections and initial prompt from config.
    Returns (corrections_dict, initial_prompt).
    """
    default_corrections = {
        "AAA": "AAA",
        "AAA": "AAA",
        "XXX-групп": "XXX Group",
    }
    default_prompt = "Розмова про продукцію компанії."
    
    if not BRANDS_CONFIG.exists():
        return default_corrections, default_prompt
    
    try:
        with open(BRANDS_CONFIG, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            corrections = config.get('corrections', default_corrections)
            prompt = config.get('initial_prompt', default_prompt)
            return corrections, prompt
    except Exception as e:
        print(f"Warning: Could not load brands config: {e}")
        return default_corrections, default_prompt

# ----------------------------
# Manager Mapping
# ----------------------------
class ManagerMapper:
    def __init__(self, config_path: Path):
        self.management_dev: Dict[str, Any] = {}
        self.sales: List[Dict[str, Any]] = []
        self.default_manager: Dict[str, str] = {
            "name": "Unknown/General",
            "id": "manager_unknown",
            "role": "unknown"
        }
        
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self.management_dev = config.get('management_dev', {})
                self.sales = config.get('sales', {}).get('managers', [])
                self.default_manager = config.get('default_manager', self.default_manager)
        else:
            print(f"Warning: Manager config not found at {config_path}")
    
    def normalize_number(self, number: str) -> str:
        """Remove all non-digit characters from phone number."""
        return re.sub(r'[^\d]', '', number)
    
    def find_manager(self, src_number: str, dst_number: str, direction: str) -> Dict[str, str]:
        """
        Find manager based on phone numbers and call direction.
        
        Logic:
        1. For outgoing: check source (who made the call)
        2. For incoming: check destination (who received the call)
        3. Check management/dev first (by extension), then sales
        """
        src_norm = self.normalize_number(src_number)
        dst_norm = self.normalize_number(dst_number)
        
        # Check management/dev managers by extension FIRST
        for mgr in self.management_dev.get('managers', []):
            internal_exts = [str(ext) for ext in mgr.get('internal_extensions', [])]
            
            if direction == "incoming":
                # Incoming: check destination extension
                if dst_number in internal_exts:
                    return {
                        "name": mgr['name'],
                        "id": mgr['id'],
                        "role": mgr.get('role', 'management')
                    }
            elif direction == "outgoing":
                # Outgoing: check source extension
                if src_number in internal_exts:
                    return {
                        "name": mgr['name'],
                        "id": mgr['id'],
                        "role": mgr.get('role', 'management')
                    }
        
        # Check if call involves management/dev shared external line (as fallback)
        mgmt_line = self.normalize_number(
            self.management_dev.get('shared_external_line', '')
        )
        
        if mgmt_line and (src_norm == mgmt_line or dst_norm == mgmt_line):
            # Call involves management/dev line but no specific extension matched
            return {
                "name": "Management (general)",
                "id": "management_general",
                "role": "management"
            }
        
        # Check sales team
        for sales_mgr in self.sales:
            internal_exts = [str(ext) for ext in sales_mgr.get('internal_extensions', [])]
            external_lines = [
                self.normalize_number(num) 
                for num in sales_mgr.get('external_lines', [])
            ]
            
            if direction == "incoming":
                # Incoming sales calls: check if destination matches
                if dst_number in internal_exts or dst_norm in external_lines:
                    return {
                        "name": sales_mgr['name'],
                        "id": sales_mgr['id'],
                        "role": "sales"
                    }
            elif direction == "outgoing":
                # Outgoing sales calls: check source extension/line
                if src_number in internal_exts or src_norm in external_lines:
                    return {
                        "name": sales_mgr['name'],
                        "id": sales_mgr['id'],
                        "role": "sales"
                    }
        
        return self.default_manager

def aggregate_report_by_manager(per_call: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate statistics per manager with role-based grouping.
    """
    managers_stats: Dict[str, Dict[str, Any]] = {}
    role_summary: Dict[str, Dict[str, int]] = {}
    
    processed = [c for c in per_call if c.get("status") == "processed"]
    
    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except Exception:
            return default
    
    for call in processed:
        meta = call.get("meta", {})
        analysis = call.get("analysis", {})
        
        manager_id = meta.get("manager_id", "manager_unknown")
        manager_name = meta.get("manager_name", "Невідомий")
        role = meta.get("role", "unknown")
        
        if manager_id not in managers_stats:
            managers_stats[manager_id] = {
                "manager_name": manager_name,
                "manager_id": manager_id,
                "role": role,
                "total_calls": 0,
                "incoming": 0,
                "outgoing": 0,
                "spam_calls": 0,
                "effective_calls": 0,
                "total_duration_seconds": 0.0,
                "intents": {},
                "outcomes": {},
                "questions": {},
            }
        
        # Track role summary
        if role not in role_summary:
            role_summary[role] = {
                "total_calls": 0,
                "effective_calls": 0,
                "spam_calls": 0
            }
        role_summary[role]["total_calls"] += 1
        
        stats = managers_stats[manager_id]
        stats["total_calls"] += 1
        
        # Direction
        direction = analysis.get("direction", "unknown")
        if direction == "incoming":
            stats["incoming"] += 1
        elif direction == "outgoing":
            stats["outgoing"] += 1
        
        # Spam
        if num(analysis.get("spam_probability", 0.0)) >= 0.7:
            stats["spam_calls"] += 1
            role_summary[role]["spam_calls"] += 1
        
        # Effective
        if analysis.get("effective_call") is True:
            stats["effective_calls"] += 1
            role_summary[role]["effective_calls"] += 1
        
        # Duration
        stats["total_duration_seconds"] += meta.get("audio_seconds", 0.0)
        
        # Intent
        intent = analysis.get("intent", "інше")
        stats["intents"][intent] = stats["intents"].get(intent, 0) + 1
        
        # Outcome
        outcome = analysis.get("outcome", "невідомо")
        stats["outcomes"][outcome] = stats["outcomes"].get(outcome, 0) + 1
        
        # Questions
        for q in analysis.get("key_questions", []) or []:
            qq = (q or "").strip()
            if qq:
                stats["questions"][qq] = stats["questions"].get(qq, 0) + 1
    
    # Sort and format per manager
    for manager_id, stats in managers_stats.items():
        stats["top_intents"] = sorted(stats["intents"].items(), key=lambda kv: kv[1], reverse=True)[:10]
        stats["top_outcomes"] = sorted(stats["outcomes"].items(), key=lambda kv: kv[1], reverse=True)[:5]
        stats["top_questions"] = sorted(stats["questions"].items(), key=lambda kv: kv[1], reverse=True)[:10]
        
        # Remove raw dicts
        del stats["intents"]
        del stats["outcomes"]
        del stats["questions"]
    
    # Group by role
    by_role: Dict[str, List[Dict[str, Any]]] = {
        "sales": [],
        "management": [],
        "development": [],
        "unknown": []
    }
    
    for stats in managers_stats.values():
        role = stats.get("role", "unknown")
        by_role[role].append(stats)
    
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "role_summary": role_summary,
        "by_role": {
            role: managers 
            for role, managers in by_role.items() 
            if managers  # Only include roles with data
        },
        "all_managers": list(managers_stats.values()),
        "total_managers": len(managers_stats),
    }

BRAND_CORRECTIONS, WHISPER_INITIAL_PROMPT = load_brand_corrections()

# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ensure_dirs()
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Load manager mapping
    manager_mapper = ManagerMapper(MANAGERS_CONFIG)

    model = WhisperModel(WHISPER_MODEL, device=DEVICE, compute_type=COMPUTE_TYPE)

    # Choose discovery method based on DAYS env var
    days_env = os.getenv("DAYS", "").strip()
    
    if days_env:
        print(f"Using DAYS filter: {days_env}")
        all_files = discover_wav_files_from_specified_dirs()
    else:
        print("No DAYS filter specified, discovering all WAV files recursively")
        all_files = discover_all_wav_files()

    print(f"Discovered {len(all_files)} total WAV files")

    # Filter to unprocessed files (unless forcing)
    files_to_process = filter_unprocessed_files(all_files)
    print(f"Found {len(files_to_process)} unprocessed files")

    # Apply limit
    if len(files_to_process) > PROCESS_LIMIT:
        print(f"Limiting to {PROCESS_LIMIT} files (set PROCESS_LIMIT to change)")
        files_to_process = files_to_process[:PROCESS_LIMIT]

    if not files_to_process:
        print("No files to process.")
        return

    per_call: List[Dict[str, Any]] = []

    for src in tqdm(files_to_process, desc="Processing"):
        meta = parse_freepbx_filename(src.name)
        meta["source_file"] = src.name
        meta["source_path"] = str(src)

        # Map to manager
        manager_info = manager_mapper.find_manager(
            meta.get("src_number", ""),
            meta.get("dst_number", ""),
            meta.get("direction", "unknown")
        )
        meta["manager_name"] = manager_info["name"]
        meta["manager_id"] = manager_info["id"]
        meta["role"] = manager_info.get("role", "unknown")
        # Map to manager

        # Skip tiny files
        if src.stat().st_size < MIN_BYTES:
            per_call.append({"meta": meta, "status": "skipped_too_small"})
            continue

        cid = sha12(src.name + str(src.stat().st_size))
        meta["call_id"] = cid

        norm_path = NORM / f"{cid}.wav"
        tr_path = TRANS / f"{cid}.json"
        an_path = ANALYSIS / f"{cid}.json"

        if not norm_path.exists():
            normalize_audio(src, norm_path)

        dur = ffprobe_duration_seconds(norm_path)
        meta["audio_seconds"] = dur

        if dur < MIN_SECONDS:
            per_call.append({"meta": meta, "status": "skipped_too_short"})
            continue

        # --- Transcribe ---
        transcript: Dict[str, Any]
        if (not FORCE_RETRANSCRIBE) and tr_path.exists():
            transcript = json.loads(tr_path.read_text(encoding="utf-8"))
        else:
            transcript = transcribe(model, norm_path)

        # Add manager info to transcript
        transcript["manager_name"] = meta["manager_name"]
        transcript["manager_id"] = meta["manager_id"]
        transcript["role"] = meta["role"]
        transcript["call_meta"] = {
            "direction": meta.get("direction"),
            "src_number": meta.get("src_number"),
            "dst_number": meta.get("dst_number"),
            "date": meta.get("date"),
            "time": meta.get("time"),
        }

        # Ensure UA transcript fields
        changed = False
        try:
            transcript, changed = ensure_transcript_uk(transcript)
        except Exception as e:
            # If translation fails, keep original text; analysis will still be forced UA
            transcript.setdefault("text_uk", transcript.get("text", ""))
            transcript.setdefault("segments_uk", [])
            transcript.setdefault("translation_error", repr(e))
            changed = True

        if FORCE_RETRANSCRIBE or changed or (not tr_path.exists()):
            tr_path.write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")

        # --- Analyze ---
        analysis: Dict[str, Any]
        if (not FORCE_REANALYZE) and an_path.exists():
            analysis = json.loads(an_path.read_text(encoding="utf-8"))
            analysis = ensure_analysis_schema(analysis, meta)
        else:
            text_uk = (transcript.get("text_uk") or transcript.get("text") or "").strip()
            try:
                analysis = ollama_analyze(meta, text_uk)
            except Exception as e:
                analysis = ensure_analysis_schema({}, meta)
                analysis["effective_call"] = False
                analysis["spam_probability"] = 1.0
                analysis["intent"] = "інше"
                analysis["outcome"] = "невідомо"
                analysis["summary"] = "Не вдалося отримати коректний JSON-аналіз від моделі. Дзвінок позначено як проблемний для повторної перевірки."
                analysis["analysis_error"] = repr(e)

        # Add manager info to analysis
        analysis["manager_name"] = meta["manager_name"]
        analysis["manager_id"] = meta["manager_id"]
        analysis["role"] = meta["role"]
        analysis["call_meta"] = {
            "direction": meta.get("direction"),
            "src_number": meta.get("src_number"),
            "dst_number": meta.get("dst_number"),
            "date": meta.get("date"),
            "time": meta.get("time"),
            "audio_seconds": meta.get("audio_seconds"),
        }

        # Always save normalized analysis back
        an_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")

        per_call.append({"meta": meta, "analysis": analysis, "status": "processed"})

    # Generate overall report
    report = aggregate_report(per_call)
    (OUT / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Generate per-manager report
    manager_report = aggregate_report_by_manager(per_call)
    (OUT / "report_by_manager.json").write_text(json.dumps(manager_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== OVERALL SUMMARY ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    
    print("\n=== PER-MANAGER SUMMARY ===")
    print(json.dumps(manager_report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
    