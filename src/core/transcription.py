from pathlib import Path
from typing import Any, Dict, List

from core.rules import correct_brand_names
from domain.config import AppConfig


def transcribe(model: Any, wav_path: Path, config: AppConfig) -> Dict[str, Any]:
    """Transcribe audio using Whisper model with config settings."""
    requested_lang = (getattr(config, "stt_language", "auto") or "").strip().lower()
    language_arg = None if requested_lang in {"", "auto", "mixed", "multilingual", "uk+ru", "ru+uk"} else requested_lang
    segments, info = model.transcribe(
        str(wav_path),
        language=language_arg,
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
