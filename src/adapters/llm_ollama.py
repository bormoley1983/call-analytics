import json
import re
import time
from typing import Any, Dict, List, Tuple

import requests

from core.rules import ensure_analysis_schema, truncate_text_for_analysis
from domain.config import AppConfig

# ----------------------------
# CONSTANTS
# ----------------------------
TRANSLATION_PROMPT_TEMPLATE = """Переклади наступні фрагменти на українську мову. Збережи нумерацію.

{combined}

Поверни ТІЛЬКИ переклад у такому ж форматі (номер. текст), без додаткових пояснень."""

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


