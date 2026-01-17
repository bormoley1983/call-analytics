import subprocess
from pathlib import Path


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
