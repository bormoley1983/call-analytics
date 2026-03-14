# decides what to process (incremental)
import os
from pathlib import Path
from typing import List

from core.rules import sha12
from domain.config import AppConfig


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
        if not day_path.resolve().is_relative_to(config.calls_raw.resolve()):
            print(f"  Skipping unsafe path: {d}")
            continue
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
