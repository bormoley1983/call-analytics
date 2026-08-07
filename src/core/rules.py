# skip/filters, brand corrections, etc.
import hashlib
import logging
import re
from typing import Any

from domain.config import AppConfig

logger = logging.getLogger(__name__)

# Lazy-loaded tiktoken encoding — avoids import-time failure if tiktoken
# cache is unavailable at module load time (e.g. parallel pytest workers).
_enc = None


def _get_encoding():
    global _enc
    if _enc is None:
        import tiktoken

        _enc = tiktoken.get_encoding(
            "cl100k_base"
        )  # closest public BPE to Qwen's tokenizer
    return _enc


# ----------------------------
# CONSTANTS
# ----------------------------
TRUNCATION_MESSAGE_UK = (
    "\n\n[... транскрипт обрізано через обмеження довжини моделі ...]"
)
VALID_INTENTS_UK = {
    "консультація",
    "скарга",
    "оформлення замовлення",
    "запит інформації",
    "інше",
}
VALID_OUTCOMES_UK = {
    "продаж",
    "консультація",
    "відмова",
    "переведення на іншого",
    "невідомо",
}


def sha12(s: str) -> str:
    """Generate 12-character hash for file identification.

    Deprecated: uses SHA-1 which is cryptographically weak.
    New code should use core.utils.call_id_hash() instead (SHA-256 based).
    Kept for backward compatibility with existing call_ids in storage.
    """
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def sha256_short(s: str) -> str:
    """Generate 12-character hash using SHA-256 (drop-in replacement for sha12)."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def correct_brand_names(text: str, corrections: dict[str, str]) -> str:
    """Replace incorrectly transcribed brand names with word boundaries."""
    corrected = text
    for wrong, correct in corrections.items():
        pattern = re.compile(rf"\b{re.escape(wrong)}\b", re.IGNORECASE)
        corrected = pattern.sub(correct, corrected)
    return corrected


def estimate_tokens(text: str) -> int:
    return len(_get_encoding().encode(text))


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
        current_tokens,
        available_tokens,
    )

    truncated = text[:max_chars]

    last_period = truncated.rfind(".")
    last_newline = truncated.rfind("\n")
    cut_point = max(last_period, last_newline)

    if cut_point < 0:
        # No period or newline found; fall back to word boundary or hard cutoff
        last_space = truncated.rfind(" ")
        cut_point = last_space if last_space > 0 else int(max_chars * 0.9)

    if cut_point > max_chars * 0.9 or cut_point >= 0:
        truncated = truncated[: cut_point + 1]

    return truncated + TRUNCATION_MESSAGE_UK


def ensure_analysis_schema(
    analysis: dict[str, Any], call_meta: dict[str, Any]
) -> dict[str, Any]:
    """Ensure analysis has all required fields with defaults."""
    defaults: dict[str, Any] = {
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
        analysis["spam_probability"] = max(
            0.0, min(1.0, float(analysis["spam_probability"]))
        )
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
