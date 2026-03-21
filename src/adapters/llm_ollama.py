import json
import logging
import re
import time
from typing import Any, Dict, List, Tuple

import requests

from core.rules import ensure_analysis_schema, truncate_text_for_analysis
from domain.config import AppConfig

logger = logging.getLogger(__name__)

# ----------------------------
# CONSTANTS
# ----------------------------
TRANSLATION_PROMPT_TEMPLATE = """Переклади наступні фрагменти на українську мову. Збережи нумерацію.

{combined}

Поверни ТІЛЬКИ переклад у такому ж форматі (номер. текст), без додаткових пояснень."""

KEYWORD_CATALOG_ANALYSIS_PROMPT_TEMPLATE = """You are analyzing a call analytics keyword catalog.

Your task is to group overlapping or closely related keywords and suggest safe, reversible cleanup actions.

Rules:
- Use only the provided keywords as evidence.
- Do not invent unsupported categories or aliases.
- Prefer conservative merge suggestions.
- Keep current `keyword_id` values whenever possible.
- If a keyword looks too generic, stale, or redundant, explain why.
- Return only JSON.

Return a JSON object with this structure:
{{
  "summary": "short summary",
  "groups": [
    {{
      "group_label": "human readable group name",
      "theme": "short theme",
      "keywords": ["keyword_id"],
      "primary_keyword_id": "keyword_id",
      "suggested_category": "category",
      "suggested_shared_terms": ["term"],
      "suggested_actions": [
        {{
          "type": "keep|merge|rename|expand_aliases|deactivate",
          "keyword_id": "keyword_id",
          "target_keyword_id": "keyword_id or empty",
          "suggested_label": "new label or empty",
          "suggested_terms": ["term"],
          "reason": "brief explanation"
        }}
      ],
      "rationale": "brief explanation"
    }}
  ],
  "ungrouped_keyword_ids": ["keyword_id"],
  "global_recommendations": ["recommendation"]
}}

Maximum groups: {max_groups}

Analysis payload:
{analysis_payload_json}
"""

class OllamaLlm:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
    
    def translate_segments_to_uk(self, segments: List[Dict[str, Any]]) -> List[str] | None:
        return translate_segments_to_uk(segments, self.config)
        
    def analyze(self, call_meta: Dict[str, Any], transcript_text_uk: str) -> Dict[str, Any]:
        return ollama_analyze(call_meta, transcript_text_uk, self.config)

    def analyze_keyword_catalog(self, analysis_payload: Dict[str, Any], max_groups: int = 20) -> Dict[str, Any]:
        return ollama_analyze_keyword_catalog(analysis_payload, self.config, max_groups=max_groups)


def _ollama_generate(prompt: str, config: AppConfig, temperature: float = 0.2, force_json: bool = False) -> str:
    """Generate text using Ollama with retry logic."""
    last_err: Exception | None = None

    payload = {
        "model": config.ollama_model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": config.ollama_keep_alive,
        "think": config.ollama_think,
        "options": {
            "temperature": temperature,
            "num_ctx": config.ollama_context_window,
        },
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
            logger.debug(
                "Ollama response metrics: model=%s prompt_eval_count=%s eval_count=%s "
                "load_duration_s=%.2f prompt_eval_duration_s=%.2f eval_duration_s=%.2f total_duration_s=%.2f",
                config.ollama_model,
                data.get("prompt_eval_count"),
                data.get("eval_count"),
                data.get("load_duration", 0) / 1_000_000_000,
                data.get("prompt_eval_duration", 0) / 1_000_000_000,
                data.get("eval_duration", 0) / 1_000_000_000,
                data.get("total_duration", 0) / 1_000_000_000,
            )
            return data.get("response", "")
        except Exception as e:
            last_err = e
            if attempt < config.ollama_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(
                    "Ollama request failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, config.ollama_retries, wait_time, e,
                )
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

        logger.warning(
            "Translation length mismatch: expected %d, got %d", len(texts), len(translated)
        )
        return None
    except Exception as e:
        logger.warning("Translation error: %s", e)
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


def _normalize_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    result: List[str] = []
    for item in value:
        normalized = str(item).strip()
        if normalized:
            result.append(normalized)
    return result


def ollama_analyze_keyword_catalog(
    analysis_payload: Dict[str, Any],
    config: AppConfig,
    max_groups: int = 20,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    prompt = KEYWORD_CATALOG_ANALYSIS_PROMPT_TEMPLATE.format(
        max_groups=max_groups,
        analysis_payload_json=json.dumps(analysis_payload, ensure_ascii=False, indent=2),
    )
    logger.info(
        "Sending keyword catalog analysis request to Ollama: keywords=%d customers=%d prompt_chars=%d timeout_s=%d",
        len(analysis_payload.get("keywords", [])),
        len(analysis_payload.get("customer_context", [])),
        len(prompt),
        config.ollama_timeout,
    )
    raw = _ollama_generate(prompt, config, temperature=0.2, force_json=True)
    logger.info(
        "Received keyword catalog analysis response from Ollama: response_chars=%d elapsed_s=%.2f",
        len(raw),
        time.perf_counter() - started_at,
    )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = _extract_json_object(raw)

    if not isinstance(data, dict):
        raise ValueError("Keyword catalog analysis response must be a JSON object")

    groups: List[Dict[str, Any]] = []
    for item in data.get("groups", []):
        if not isinstance(item, dict):
            continue
        actions: List[Dict[str, Any]] = []
        for action in item.get("suggested_actions", []):
            if not isinstance(action, dict):
                continue
            actions.append(
                {
                    "type": str(action.get("type", "")).strip(),
                    "keyword_id": str(action.get("keyword_id", "")).strip(),
                    "target_keyword_id": str(action.get("target_keyword_id", "")).strip(),
                    "suggested_label": str(action.get("suggested_label", "")).strip(),
                    "suggested_terms": _normalize_string_list(action.get("suggested_terms")),
                    "reason": str(action.get("reason", "")).strip(),
                }
            )
        groups.append(
            {
                "group_label": str(item.get("group_label", "")).strip(),
                "theme": str(item.get("theme", "")).strip(),
                "keywords": _normalize_string_list(item.get("keywords")),
                "primary_keyword_id": str(item.get("primary_keyword_id", "")).strip(),
                "suggested_category": str(item.get("suggested_category", "")).strip(),
                "suggested_shared_terms": _normalize_string_list(item.get("suggested_shared_terms")),
                "suggested_actions": actions,
                "rationale": str(item.get("rationale", "")).strip(),
            }
        )

    return {
        "summary": str(data.get("summary", "")).strip(),
        "groups": groups,
        "ungrouped_keyword_ids": _normalize_string_list(data.get("ungrouped_keyword_ids")),
        "global_recommendations": _normalize_string_list(data.get("global_recommendations")),
    }
