# skip/filters, brand corrections, etc.
import hashlib
import logging
import re
from typing import Any, Dict

from domain.config import AppConfig

logger = logging.getLogger(__name__)

# ----------------------------
# CONSTANTS
# ----------------------------
TRUNCATION_MESSAGE_UK = "\n\n[... транскрипт обрізано через обмеження довжини моделі ...]"
VALID_INTENTS_UK = {"консультація", "скарга", "оформлення замовлення", "запит інформації", "інше"}
VALID_OUTCOMES_UK = {"продаж", "консультація", "відмова", "переведення на іншого", "невідомо"}

def sha12(s: str) -> str:
    """Generate 12-character hash for file identification."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def correct_brand_names(text: str, corrections: Dict[str, str]) -> str:
    """Replace incorrectly transcribed brand names with word boundaries."""
    corrected = text
    for wrong, correct in corrections.items():
        pattern = re.compile(rf'\b{re.escape(wrong)}\b', re.IGNORECASE)
        corrected = pattern.sub(correct, corrected)
    return corrected


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

    logger.warning(
        "Transcript too long (%d tokens estimated). Truncating to %d tokens.",
        current_tokens, available_tokens,
    )
    
    truncated = text[:max_chars]
    
    last_period = truncated.rfind('.')
    last_newline = truncated.rfind('\n')
    cut_point = max(last_period, last_newline)
    
    if cut_point > max_chars * 0.9:
        truncated = truncated[:cut_point + 1]
    
    return truncated + TRUNCATION_MESSAGE_UK


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

    # ← insert here, after defaults are filled so fields are guaranteed to exist
    try:
        analysis["spam_probability"] = max(0.0, min(1.0, float(analysis["spam_probability"])))
    except (TypeError, ValueError):
        analysis["spam_probability"] = 0.0

    ec = analysis["effective_call"]
    if isinstance(ec, str):
        analysis["effective_call"] = ec.lower() in ("true", "1", "yes", "так")
    else:
        analysis["effective_call"] = bool(ec)

    if analysis["intent"] not in VALID_INTENTS_UK:
        analysis["intent"] = "інше"

    if analysis["outcome"] not in VALID_OUTCOMES_UK:
        analysis["outcome"] = "невідомо"

    if not isinstance(analysis["key_questions"], list):
        analysis["key_questions"] = []

    if not isinstance(analysis["objections"], list):
        analysis["objections"] = []

    return analysis
