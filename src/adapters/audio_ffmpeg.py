import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class FfmpegAudio:
    def normalize(self, src: Path, dst: Path) -> None:
        normalize_audio(src, dst)

    def duration_seconds(self, path: Path) -> float:
        return ffprobe_duration_seconds(path)


def ffprobe_duration_seconds(path: Path) -> float:
    """Get audio duration using ffprobe.

    Returns 0.0 if ffprobe fails or the output is not a valid number,
    but logs the error for debugging.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.error("ffprobe failed for %s: %s", path, exc)
        return 0.0

    if p.returncode != 0:
        logger.warning("ffprobe returned non-zero for %s: %s", path, p.stderr.strip())
        return 0.0
    try:
        return float(p.stdout.strip())
    except (ValueError, TypeError):
        logger.warning(
            "ffprobe returned non-numeric duration for %s: %r", path, p.stdout
        )
        return 0.0


def normalize_audio(src: Path, dst: Path) -> None:
    """Convert to 16kHz mono wav (better for Whisper)."""
    cmd = ["ffmpeg", "-y", "-i", str(src), "-ac", "1", "-ar", "16000", "-vn", str(dst)]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
    except subprocess.CalledProcessError as exc:
        logger.error("ffmpeg normalize failed for %s: stderr=%s", src, exc.stderr[:500])
        raise
    except subprocess.TimeoutExpired as exc:
        logger.error("ffmpeg normalize timed out for %s after 300s", src)
        raise RuntimeError(f"ffmpeg normalize timed out for {src}") from exc
