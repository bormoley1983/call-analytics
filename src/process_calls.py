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
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
from tqdm import tqdm
from faster_whisper import WhisperModel

from call_config import AppConfig, load_app_config


# ----------------------------
# CONSTANTS
# ----------------------------
TRUNCATION_MESSAGE_UK = "\n\n[... транскрипт обрізано через обмеження довжини моделі ...]"
TRANSLATION_PROMPT_TEMPLATE = """Переклади наступні фрагменти на українську мову. Збережи нумерацію.

{combined}

Поверни ТІЛЬКИ переклад у такому ж форматі (номер. текст), без додаткових пояснень."""

# ----------------------------
# Helpers - Now Accept Config
# ----------------------------
def ensure_dirs(config: AppConfig) -> None:
    """Ensure output directories exist."""
    for p in [config.out, config.norm, config.trans, config.analysis]:
        p.mkdir(parents=True, exist_ok=True)


def sha12(s: str) -> str:
    """Generate 12-character hash for file identification."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def discover_all_wav_files(config: AppConfig) -> List[Path]:
    """
    Recursively discover all .wav files under CALLS_RAW directory.
    Returns them sorted by modification time (oldest first) for consistent processing.
    """
    if not config.calls_raw.exists():
        return []
    
    all_files = list(config.calls_raw.rglob("*.wav"))
    # Sort by modification time (oldest first)
    all_files.sort(key=lambda p: p.stat().st_mtime)
    return all_files


def discover_wav_files_from_specified_dirs(config: AppConfig) -> List[Path]:
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
        day_path = config.calls_raw / d
        if day_path.exists():
            all_files.extend(day_path.glob("*.wav"))

    all_files = sorted(all_files)
    
    if not all_files:
        print("No WAV files found. Checked day folders under:", config.calls_raw)
        for d in day_list:
            print("  ", (config.calls_raw / d))
        return []

    return all_files


def filter_unprocessed_files(files: List[Path], config: AppConfig) -> List[Path]:
    """
    Filter out files that have already been processed.
    A file is considered processed if both transcript and analysis exist.
    """
    unprocessed = []
    for src in files:
        cid = sha12(src.name + str(src.stat().st_size))
        tr_path = config.trans / f"{cid}.json"
        an_path = config.analysis / f"{cid}.json"
        
        # If forcing re-processing, include all
        if config.force_retranscribe or config.force_reanalyze:
            unprocessed.append(src)
        # Otherwise only include if not fully processed
        elif not (tr_path.exists() and an_path.exists()):
            unprocessed.append(src)
    
    return unprocessed


def parse_freepbx_filename(name: str) -> Dict[str, Any]:
    """Parse FreePBX filename format: dir-dst-src-YYYYMMDD-HHMMSS-uniqueid.wav"""
    base = name.rsplit(".", 1)[0] if "." in name else name
    parts = base.split("-")
    meta: Dict[str, Any] = {"raw_name": name}

    if len(parts) < 6:
        meta["direction"] = "unknown"
        return meta

    dir_tag, dst, src, yyyymmdd, hhmmss = parts[0], parts[1], parts[2], parts[3], parts[4]
    uniqueid = "-".join(parts[5:])

    direction = "incoming" if dir_tag == "in" else "outgoing" if dir_tag == "out" else "unknown"

    meta.update({
        "direction": direction,
        "dst_number": dst,
        "src_number": src,
        "date": yyyymmdd,
        "time": hhmmss,
        "asterisk_uniqueid": uniqueid,
    })
    return meta


def ffprobe_duration_seconds(path: Path) -> float:
    """Get audio duration using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
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
    """Convert to 16kHz mono wav (better for Whisper)."""
    cmd = ["ffmpeg", "-y", "-i", str(src), "-ac", "1", "-ar", "16000", "-vn", str(dst)]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def transcribe(model: WhisperModel, wav_path: Path, config: AppConfig) -> Dict[str, Any]:
    """Transcribe audio using Whisper model with config settings."""
    segments, info = model.transcribe(
        str(wav_path),
        language="uk",
        initial_prompt=config.whisper_initial_prompt,
        vad_filter=True,
        beam_size=config.whisper_beam_size,
        word_timestamps=False,
    )
    seg_list: List[Dict[str, Any]] = []
    full_text: List[str] = []

    for s in segments:
        t = (s.text or "").strip()
        if not t:
            continue
        
        # Apply brand name corrections
        t = correct_brand_names(t, config.brand_corrections)
        
        seg_list.append({"start": float(s.start), "end": float(s.end), "text": t})
        full_text.append(t)

    return {
        "language": info.language,
        "duration": float(info.duration),
        "segments": seg_list,
        "text": "\n".join(full_text).strip(),
    }


def estimate_tokens(text: str) -> int:
    """Rough estimation of tokens for Ukrainian/Cyrillic text (~2 chars per token)."""
    return len(text) // 2


def truncate_text_for_analysis(text: str, config: AppConfig) -> str:
    """
    Truncate text to fit within model's context window.
    Reserve space for system prompt, JSON schema, and response.
    """
    available_tokens = config.ollama_context_window - config.ollama_token_overhead
    max_chars = available_tokens * 2  # ~2 chars per token for Ukrainian
    
    current_tokens = estimate_tokens(text)
    
    if current_tokens <= available_tokens:
        return text
    
    print(f"Warning: Transcript too long ({current_tokens} tokens estimated). Truncating to {available_tokens} tokens.")
    
    truncated = text[:max_chars]
    
    # Try to truncate at sentence boundary
    last_period = truncated.rfind('.')
    last_newline = truncated.rfind('\n')
    cut_point = max(last_period, last_newline)
    
    if cut_point > max_chars * 0.9:
        truncated = truncated[:cut_point + 1]
    
    return truncated + TRUNCATION_MESSAGE_UK


def correct_brand_names(text: str, corrections: Dict[str, str]) -> str:
    """Replace incorrectly transcribed brand names with word boundaries."""
    corrected = text
    for wrong, correct in corrections.items():
        pattern = re.compile(rf'\b{re.escape(wrong)}\b', re.IGNORECASE)
        corrected = pattern.sub(correct, corrected)
    return corrected


def _ollama_generate(prompt: str, config: AppConfig, temperature: float = 0.2, force_json: bool = False) -> str:
    """Generate text using Ollama with retry logic."""
    last_err: Exception | None = None

    payload = {
        "model": config.ollama_model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }
    if force_json:
        payload["format"] = "json"

    for attempt in range(config.ollama_retries):
        try:
            r = requests.post(
                f"{config.ollama_url}/api/generate",
                json=payload,
                timeout=config.ollama_timeout
            )
            r.raise_for_status()
            data = r.json()
            return data.get("response", "")
        except Exception as e:
            last_err = e
            if attempt < config.ollama_retries - 1:
                wait_time = 2 ** attempt
                print(f"Ollama request failed (attempt {attempt+1}/{config.ollama_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)

    raise RuntimeError(f"Ollama request failed after {config.ollama_retries} retries: {last_err!r}")


def _extract_json_object(raw: str) -> Dict[str, Any]:
    """Extract JSON object from text response."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in response")
    return json.loads(m.group(0))


def translate_segments_to_uk(segments: List[Dict[str, Any]], config: AppConfig) -> List[str] | None:
    """
    Translate segment texts to Ukrainian in a single call.
    Returns list of translated strings in same order, or None if too large.
    """
    if not config.force_translate_uk:
        return None
    
    if len(segments) > config.max_segments_translate:
        return None
    
    texts = [seg.get("text", "").strip() for seg in segments if seg.get("text")]
    if not texts:
        return None
    
    combined = "\n".join(f"{i+1}. {t}" for i, t in enumerate(texts))
    if len(combined) > config.max_chars_translate:
        return None
    
    prompt = TRANSLATION_PROMPT_TEMPLATE.format(combined=combined)
    
    try:
        raw = _ollama_generate(prompt, config, temperature=0.1, force_json=False)
        lines = [ln.strip() for ln in raw.strip().split("\n") if ln.strip()]
        
        translated = []
        for ln in lines:
            match = re.match(r"^\d+\.\s*(.+)$", ln)
            if match:
                translated.append(match.group(1))
        
        if len(translated) == len(texts):
            return translated

        print(f"Translation length mismatch: expected {len(texts)}, got {len(translated)}")
        return None
    except Exception as e:
        print(f"Translation error: {e}")
        return None


def ensure_transcript_uk(transcript: Dict[str, Any], config: AppConfig) -> Tuple[Dict[str, Any], bool]:
    """
    Ensure transcript has Ukrainian text fields.
    Returns (updated_transcript, changed_flag).
    """
    changed = False
    
    if "text_uk" not in transcript or not transcript["text_uk"]:
        if config.force_translate_uk:
            segments = transcript.get("segments", [])
            translated = translate_segments_to_uk(segments, config)
            
            if translated:
                transcript["text_uk"] = "\n".join(translated)
                transcript["segments_uk"] = [
                    {"start": seg["start"], "end": seg["end"], "text": uk_text}
                    for seg, uk_text in zip(segments, translated)
                ]
                changed = True
            else:
                transcript["text_uk"] = transcript.get("text", "")
                transcript["segments_uk"] = transcript.get("segments", [])
                changed = True
        else:
            transcript["text_uk"] = transcript.get("text", "")
            transcript["segments_uk"] = transcript.get("segments", [])
            changed = True
    
    return transcript, changed


def ollama_analyze(call_meta: Dict[str, Any], transcript_text_uk: str, config: AppConfig) -> Dict[str, Any]:
    """
    Analyze call via Ollama in Ukrainian, expecting a JSON response.
    """
    # Truncate if needed
    t = truncate_text_for_analysis(transcript_text_uk, config)
    
    direction = call_meta.get("direction", "unknown")
    src_num = call_meta.get("src_number", "")
    dst_num = call_meta.get("dst_number", "")
    
    # Get company info from config
    company_info = config.analysis_config.get("company", {})
    company_name = company_info.get("name", "компанія")
    business = company_info.get("business", "продукцію")
    
    # Get prompt template from config
    prompt_template = config.analysis_config.get("analysis_prompt", "")
    
    prompt = prompt_template.format(
        company_name=company_name,
        business=business,
        direction=direction,
        src_number=src_num,
        dst_number=dst_num,
        transcript=t
    )
    
    raw = _ollama_generate(prompt, config, temperature=0.3, force_json=True)
    
    try:
        analysis = json.loads(raw)
    except json.JSONDecodeError:
        analysis = _extract_json_object(raw)

    return ensure_analysis_schema(analysis, call_meta)


def ensure_analysis_schema(analysis: Dict[str, Any], call_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure analysis has all required fields with defaults."""
    defaults: Dict[str, Any] = {
        "spam_probability": 0.0,
        "effective_call": False,
        "intent": "інше",
        "direction": call_meta.get("direction", "unknown"),
        "outcome": "невідомо",
        "key_questions": [],
        "objections": [],
        "summary": "",
    }
    
    for key, default_val in defaults.items():
        if key not in analysis:
            analysis[key] = default_val
    
    return analysis


def aggregate_report(per_call: List[Dict[str, Any]], config: AppConfig) -> Dict[str, Any]:
    """Aggregate overall statistics from all calls."""
    processed = [c for c in per_call if c.get("status") == "processed"]
    
    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return default
    
    total_calls = len(per_call)
    transcribed = len(processed)
    skipped_small = len([c for c in per_call if c.get("status") == "skipped_too_small"])
    skipped_short = len([c for c in per_call if c.get("status") == "skipped_too_short"])
    
    spam = sum(1 for c in processed 
               if num((c.get("analysis") or {}).get("spam_probability", 0.0)) >= config.spam_probability_threshold)
    effective = sum(1 for c in processed 
                    if (c.get("analysis") or {}).get("effective_call") is True)
    
    total_duration = sum(c.get("meta", {}).get("audio_seconds", 0.0) for c in processed)
    
    intents: Dict[str, int] = {}
    outcomes: Dict[str, int] = {}
    questions: Dict[str, int] = {}
    
    for c in processed:
        analysis = c.get("analysis", {})
        
        intent = analysis.get("intent", "інше")
        intents[intent] = intents.get(intent, 0) + 1
        
        outcome = analysis.get("outcome", "невідомо")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        
        for q in analysis.get("key_questions", []) or []:
            q_lower = q.lower().strip()
            if q_lower:
                questions[q_lower] = questions.get(q_lower, 0) + 1
    
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_calls": total_calls,
        "transcribed": transcribed,
        "skipped_too_small": skipped_small,
        "skipped_too_short": skipped_short,
        "spam_calls": spam,
        "effective_calls": effective,
        "total_duration_seconds": total_duration,
        "top_intents": sorted(intents.items(), key=lambda kv: kv[1], reverse=True)[:10],
        "top_outcomes": sorted(outcomes.items(), key=lambda kv: kv[1], reverse=True)[:5],
        "top_questions": sorted(questions.items(), key=lambda kv: kv[1], reverse=True)[:10],
    }


def aggregate_report_by_manager(per_call: List[Dict[str, Any]], config: AppConfig) -> Dict[str, Any]:
    """Aggregate statistics per manager with role-based grouping."""
    managers_stats: Dict[str, Dict[str, Any]] = {}
    role_summary: Dict[str, Dict[str, int]] = {}
    
    processed = [c for c in per_call if c.get("status") == "processed"]
    
    def num(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return default
    
    for call in processed:
        meta = call.get("meta", {})
        analysis = call.get("analysis", {})
        
        manager_id = meta.get("manager_id", "manager_unknown")
        manager_name = meta.get("manager_name", "Невідомий")
        role = meta.get("role", "unknown")
        
        if manager_id not in managers_stats:
            managers_stats[manager_id] = {
                "manager_id": manager_id,
                "manager_name": manager_name,
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
            role_summary[role] = {"total_calls": 0}
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
        if num(analysis.get("spam_probability", 0.0)) >= config.spam_probability_threshold:
            stats["spam_calls"] += 1
        
        # Effective
        if analysis.get("effective_call") is True:
            stats["effective_calls"] += 1
        
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
            q_lower = q.lower().strip()
            if q_lower:
                stats["questions"][q_lower] = stats["questions"].get(q_lower, 0) + 1
    
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
            if managers
        },
        "all_managers": list(managers_stats.values()),
        "total_managers": len(managers_stats),
    }


# ----------------------------
# Main Processing Phases
# ----------------------------
def discover_and_filter_files(config: AppConfig) -> List[Path]:
    """Discover and filter WAV files based on DAYS env var and processing status."""
    days_env = os.getenv("DAYS", "").strip()
    
    if days_env:
        print(f"Using DAYS filter: {days_env}")
        all_files = discover_wav_files_from_specified_dirs(config)
    else:
        print("No DAYS filter specified, discovering all WAV files recursively")
        all_files = discover_all_wav_files(config)

    print(f"Discovered {len(all_files)} total WAV files")

    # Filter to unprocessed files (unless forcing)
    files_to_process = filter_unprocessed_files(all_files, config)
    print(f"Found {len(files_to_process)} unprocessed files")

    # Apply limit
    if len(files_to_process) > config.process_limit:
        print(f"Limiting to {config.process_limit} files (set PROCESS_LIMIT to change)")
        files_to_process = files_to_process[:config.process_limit]

    return files_to_process


def run_transcription_phase(files: List[Path], config: AppConfig) -> List[Dict[str, Any]]:
    """
    Phase 1: Transcription with Whisper (GPU intensive).
    Returns metadata for all files including skipped ones.
    """
    if not files:
        print("No files to process.")
        return []
    
    print("\n" + "="*80)
    print("PHASE 1: TRANSCRIPTION (Whisper)")
    print("="*80)
    
    model = WhisperModel(
        config.whisper_model,
        device=config.whisper_device,
        compute_type=config.whisper_compute_type
    )
    files_metadata: List[Dict[str, Any]] = []

    for src in tqdm(files, desc="Transcribing"):
        meta = parse_freepbx_filename(src.name)
        meta["source_file"] = src.name
        meta["source_path"] = str(src)

        # Map to manager
        manager_info = config.manager_mapper.find_manager(
            meta.get("src_number", ""),
            meta.get("dst_number", ""),
            meta.get("direction", "unknown")
        )
        meta["manager_name"] = manager_info["name"]
        meta["manager_id"] = manager_info["id"]
        meta["role"] = manager_info.get("role", "unknown")

        # Skip tiny files
        if src.stat().st_size < config.min_bytes:
            meta["status"] = "skipped_too_small"
            files_metadata.append(meta)
            continue

        cid = sha12(src.name + str(src.stat().st_size))
        meta["call_id"] = cid

        norm_path = config.norm / f"{cid}.wav"
        tr_path = config.trans / f"{cid}.json"
        an_path = config.analysis / f"{cid}.json"

        if not norm_path.exists():
            normalize_audio(src, norm_path)

        dur = ffprobe_duration_seconds(norm_path)
        meta["audio_seconds"] = dur

        if dur < config.min_seconds:
            meta["status"] = "skipped_too_short"
            files_metadata.append(meta)
            continue

        # Transcribe
        transcript: Dict[str, Any]
        if (not config.force_retranscribe) and tr_path.exists():
            transcript = json.loads(tr_path.read_text(encoding="utf-8"))
        else:
            transcript = transcribe(model, norm_path, config)

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
            transcript, changed = ensure_transcript_uk(transcript, config)
        except Exception as e:
            transcript.setdefault("text_uk", transcript.get("text", ""))
            transcript.setdefault("segments_uk", [])
            transcript.setdefault("translation_error", repr(e))
            changed = True

        if config.force_retranscribe or changed or (not tr_path.exists()):
            tr_path.write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")

        meta["status"] = "transcribed"
        meta["tr_path"] = str(tr_path)
        meta["an_path"] = str(an_path)
        files_metadata.append(meta)

    # Free Whisper model from memory
    del model
    transcribed_count = len([m for m in files_metadata if m.get('status') == 'transcribed'])
    print(f"\n✓ Transcription complete. Processed {transcribed_count} files.")
    print("Whisper model released from memory.")
    
    return files_metadata


def run_analysis_phase(files_metadata: List[Dict[str, Any]], config: AppConfig) -> List[Dict[str, Any]]:
    """
    Phase 2: Analysis with Ollama (different GPU usage pattern).
    Returns per-call results including analysis.
    """
    print("\n" + "="*80)
    print("PHASE 2: ANALYSIS (Ollama)")
    print("="*80)
    
    per_call: List[Dict[str, Any]] = []

    for meta in tqdm([m for m in files_metadata if m.get("status") == "transcribed"], desc="Analyzing"):
        tr_path = Path(meta["tr_path"])
        an_path = Path(meta["an_path"])
        
        # Load transcript
        transcript = json.loads(tr_path.read_text(encoding="utf-8"))
        
        # Analyze
        analysis: Dict[str, Any]
        if (not config.force_reanalyze) and an_path.exists():
            analysis = json.loads(an_path.read_text(encoding="utf-8"))
            analysis = ensure_analysis_schema(analysis, meta)
        else:
            text_uk = (transcript.get("text_uk") or transcript.get("text") or "").strip()
            try:
                analysis = ollama_analyze(meta, text_uk, config)
            except Exception as e:
                analysis = ensure_analysis_schema({}, meta)
                analysis["effective_call"] = False
                analysis["spam_probability"] = 1.0
                analysis["intent"] = "інше"
                analysis["outcome"] = "невідомо"
                analysis["summary"] = "Не вдалося отримати коректний JSON-аналіз від моделі."
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

        # Always save normalized analysis
        an_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
        per_call.append({"meta": meta, "analysis": analysis, "status": "processed"})
    
    # Add skipped files to per_call for report
    for meta in files_metadata:
        if meta.get("status") in ("skipped_too_small", "skipped_too_short"):
            per_call.append({"meta": meta, "status": meta["status"]})

    print(f"\n✓ Analysis complete. Processed {len([c for c in per_call if c.get('status') == 'processed'])} calls.")
    
    return per_call


def generate_reports(per_call: List[Dict[str, Any]], config: AppConfig) -> None:
    """Generate and save analysis reports."""
    print("\n" + "="*80)
    print("GENERATING REPORTS")
    print("="*80)
    
    # Generate overall report
    report = aggregate_report(per_call, config)
    (config.out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Generate per-manager report
    manager_report = aggregate_report_by_manager(per_call, config)
    (config.out / "report_by_manager.json").write_text(json.dumps(manager_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== OVERALL SUMMARY ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    
    print("\n=== PER-MANAGER SUMMARY ===")
    print(json.dumps(manager_report, ensure_ascii=False, indent=2))
    
    print(f"\n✓ Reports saved:")
    print(f"  - {config.out / 'report.json'}")
    print(f"  - {config.out / 'report_by_manager.json'}")


def main() -> None:
    """Main entry point for call analytics processing."""
    # Load configuration (single source of truth)
    config = load_app_config()
    
    # Setup directories
    ensure_dirs(config)

    # Discover and filter files
    files_to_process = discover_and_filter_files(config)
    
    if not files_to_process:
        print("No files to process.")
        return

    # Phase 1: Transcription (Whisper - GPU intensive)
    files_metadata = run_transcription_phase(files_to_process, config)

    # Phase 2: Analysis (Ollama - different GPU pattern)
    per_call = run_analysis_phase(files_metadata, config)

    # Phase 3: Generate reports
    generate_reports(per_call, config)
    
    print("\n" + "="*80)
    print("✓ PROCESSING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
