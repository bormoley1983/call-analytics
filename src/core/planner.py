# decides what to process (incremental)
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from core.rules import sha12
from domain.config import AppConfig

logger = logging.getLogger(__name__)


def _stat_cache(files: List[Path]) -> Dict[Path, os.stat_result]:
    cache: Dict[Path, os.stat_result] = {}
    for p in files:
        try:
            cache[p] = p.stat()
        except OSError:
            pass
    return cache


def discover_all_wav_files(config: AppConfig, stat_cache: Dict[Path, os.stat_result]) -> List[Path]:
    """
    Recursively discover all .wav files under CALLS_RAW directory.
    Populates stat_cache in-place; returns files sorted by mtime (oldest first).
    """
    if not config.calls_raw.exists():
        return []
    all_files = list(config.calls_raw.rglob("*.wav"))
    for p in all_files:
        try:
            stat_cache[p] = p.stat()
        except OSError:
            pass
    all_files.sort(key=lambda p: stat_cache[p].st_mtime if p in stat_cache else 0)
    return all_files


def discover_wav_files_from_specified_dirs(config: AppConfig, stat_cache: Dict[Path, os.stat_result]) -> List[Path]:
    """
    Discover WAV files from specific date directories.
    Expects DAYS env var: "2026/01/01,2026/01/02,..."
    Returns sorted list of .wav files from those directories.
    """
    days_env = os.getenv("DAYS", "")
    if not days_env.strip():
        logger.debug("DAYS env var not set or empty. Returning empty list.")
        return []
    day_list = [d.strip().replace("\\", "/") for d in days_env.split(",") if d.strip()]
    all_files: List[Path] = []

    for d in day_list:
        day_path = config.calls_raw / d
        if not day_path.resolve().is_relative_to(config.calls_raw.resolve()):
            logger.warning("Skipping unsafe path: %s", d)
            continue
        if day_path.exists():
            all_files.extend(day_path.glob("*.wav"))
    
    if not all_files:
        logger.warning(
            "No WAV files found. Checked day folders under: %s; dirs checked: %s",
            config.calls_raw,
            [str(config.calls_raw / d) for d in day_list],
        )
        return []

    for p in all_files:
        try:
            stat_cache[p] = p.stat()
        except OSError:
            pass

    return sorted(all_files)


def filter_unprocessed_files(files: List[Path], config: AppConfig, stat_cache: Dict[Path, os.stat_result]) -> List[Path]:
    """
    Filter out files that have already been processed.
    A file is considered processed if both transcript and analysis exist.
    """
    unprocessed = []
    for src in files:
        st = stat_cache.get(src) or src.stat()
        cid = sha12(src.name + str(st.st_size))
        tr_path = config.trans / f"{cid}.json"
        an_path = config.analysis / f"{cid}.json"
        if config.force_retranscribe or config.force_reanalyze:
            unprocessed.append(src)
        elif not (tr_path.exists() and an_path.exists()):
            unprocessed.append(src)
    
    return unprocessed


def discover_and_filter_files(config: AppConfig) -> List[Path]:
    """Discover and filter WAV files based on DAYS env var and processing status."""
    days_env = os.getenv("DAYS", "").strip()
    stat_cache: Dict[Path, os.stat_result] = {}
    
    if days_env:
        logger.info("Using DAYS filter: %s", days_env)
        all_files = discover_wav_files_from_specified_dirs(config, stat_cache)
    else:
        logger.info("No DAYS filter specified, discovering all WAV files recursively")
        all_files = discover_all_wav_files(config, stat_cache)

    logger.info("Discovered %d total WAV file(s)", len(all_files))

    # Filter to unprocessed files (unless forcing)
    files_to_process = filter_unprocessed_files(all_files, config, stat_cache)
    logger.info("Found %d unprocessed file(s)", len(files_to_process))

    # Apply limit (0 means unlimited)
    if config.process_limit > 0 and len(files_to_process) > config.process_limit:
        logger.info("Limiting to %d file(s) (set PROCESS_LIMIT to change)", config.process_limit)
        files_to_process = files_to_process[:config.process_limit]

    return files_to_process

def categorize_files(files: List[Path], config: AppConfig, stat_cache: Optional[Dict[Path, os.stat_result]] = None) -> Tuple[List[Path], List[Path]]:
    """
    Split files into:
    - needs_pipeline: require transcription/translation (go through run_transcription_phase)
    - analysis_only: already translated, only need run_analysis_phase
    """
    
    if stat_cache is None:
        stat_cache = _stat_cache(files)
    
    needs_pipeline: List[Path] = []
    analysis_only: List[Path] = []

    for src in files:
        st = stat_cache.get(src) or src.stat()
        cid = sha12(src.name + str(st.st_size))
        tr_path = config.trans / f"{cid}.json"
        # an_path = config.analysis / f"{cid}.json"

        if config.force_retranscribe or config.force_translate_uk:
            needs_pipeline.append(src)
            continue

        if tr_path.exists():
            try:
                stage = json.loads(tr_path.read_text(encoding="utf-8")).get("_pipeline_stage", "")
            except Exception:
                stage = ""

            if stage == "translated":
                analysis_only.append(src)
            else:
                needs_pipeline.append(src)
        else:
            needs_pipeline.append(src)

    return needs_pipeline, analysis_only