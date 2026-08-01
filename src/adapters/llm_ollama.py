import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Tuple

import requests

from core.rules import ensure_analysis_schema, truncate_text_for_analysis
from domain.config import AppConfig

logger = logging.getLogger(__name__)


class _RateLimiter:
    """Token-bucket rate limiter for Ollama requests.

    Prevents multiple analysis workers from overwhelming the LLM server
    by limiting concurrent in-flight requests and enforcing a minimum
    interval between requests.

    Configuration (environment variables):
        OLLAMA_RATE_LIMIT: maximum concurrent requests (default: 4, 0=disabled)
        OLLAMA_RATE_INTERVAL: minimum seconds between requests (default: 0.5)
    """

    def __init__(self) -> None:
        self._max_concurrent = _parse_int_env("OLLAMA_RATE_LIMIT", 4)
        self._interval = float(os.getenv("OLLAMA_RATE_INTERVAL", "0.5"))
        self._active = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def acquire(self) -> None:
        """Block until a slot is available, then acquire it."""
        if self._max_concurrent <= 0:
            return  # Rate limiting disabled

        with self._condition:
            while self._active >= self._max_concurrent:
                self._condition.wait()
            self._active += 1

    def release(self) -> None:
        """Release a slot and optionally wait before allowing the next request."""
        if self._max_concurrent <= 0:
            return

        with self._condition:
            self._active -= 1
            self._condition.notify()

        # Enforce minimum interval between requests
        if self._interval > 0:
            time.sleep(self._interval)


def _parse_int_env(key: str, default: int) -> int:
    """Parse an environment variable as an integer, falling back to a default."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid %s=%r (not an integer), using default %d", key, raw, default
        )
        return default


_ollama_rate_limiter = _RateLimiter()

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

    def translate_segments_to_uk(
        self, segments: List[Dict[str, Any]]
    ) -> List[str] | None:
        return translate_segments_to_uk(segments, self.config)

    def analyze(
        self, call_meta: Dict[str, Any], transcript_text_uk: str
    ) -> Dict[str, Any]:
        return ollama_analyze(call_meta, transcript_text_uk, self.config)

    def analyze_keyword_catalog(
        self, analysis_payload: Dict[str, Any], max_groups: int = 20
    ) -> Dict[str, Any]:
        return ollama_analyze_keyword_catalog(
            analysis_payload, self.config, max_groups=max_groups
        )

    def enrich_candidates(
        self, candidates: List[Dict[str, Any]], max_aliases: int = 3
    ) -> Dict[str, Any]:
        return ollama_enrich_candidates(candidates, self.config, max_aliases)

    def expand_aliases(
        self,
        *,
        keyword_id: str,
        label: str,
        current_terms: List[str],
        evidence_texts: List[str],
        max_aliases: int,
    ) -> Dict[str, Any]:
        return ollama_expand_aliases(
            keyword_id, label, current_terms, evidence_texts, max_aliases, self.config
        )

    def generate_deep_insights(
        self, insight_type: str, analysis_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        return ollama_generate_deep_insights(
            insight_type, analysis_records, self.config
        )


def _ollama_generate(
    prompt: str, config: AppConfig, temperature: float = 0.2, force_json: bool = False
) -> str:
    """Generate text using Ollama with retry logic and rate limiting."""
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
        _ollama_rate_limiter.acquire()
        try:
            r = requests.post(
                f"{config.ollama_url}/api/generate",
                json=payload,
                timeout=config.ollama_timeout,
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
        finally:
            _ollama_rate_limiter.release()
        if attempt < config.ollama_retries - 1:
            wait_time = 2**attempt
            logger.warning(
                "Ollama request failed (attempt %d/%d), retrying in %ds: %s",
                attempt + 1,
                config.ollama_retries,
                wait_time,
                last_err,
            )
            time.sleep(wait_time)

    raise RuntimeError(
        f"Ollama request failed after {config.ollama_retries} retries: {last_err!r}"
    )


def _extract_json_object(raw: str) -> Dict[str, Any]:
    """Extract JSON object from text response.

    Uses a state machine that tracks brace depth while respecting string
    literals (so braces inside quoted strings don't affect the count)
    and backslash escapes.
    """
    start = raw.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")

    depth = 0
    in_string = False
    escaped = False
    end = -1

    for i in range(start, len(raw)):
        ch = raw[i]
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            if in_string:
                escaped = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end == -1:
        raise ValueError("No valid JSON object found in response")
    return json.loads(raw[start:end])


def translate_segments_to_uk(
    segments: List[Dict[str, Any]], config: AppConfig
) -> List[str] | None:
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
            "Translation length mismatch: expected %d, got %d",
            len(texts),
            len(translated),
        )
        return None
    except Exception as e:
        logger.warning("Translation error: %s", e)
        return None


def ensure_transcript_uk(
    transcript: Dict[str, Any], config: AppConfig
) -> Tuple[Dict[str, Any], bool]:
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


def ollama_analyze(
    call_meta: Dict[str, Any], transcript_text_uk: str, config: AppConfig
) -> Dict[str, Any]:
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
        transcript=t,
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
        analysis_payload_json=json.dumps(
            analysis_payload, ensure_ascii=False, indent=2
        ),
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
                    "target_keyword_id": str(
                        action.get("target_keyword_id", "")
                    ).strip(),
                    "suggested_label": str(action.get("suggested_label", "")).strip(),
                    "suggested_terms": _normalize_string_list(
                        action.get("suggested_terms")
                    ),
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
                "suggested_shared_terms": _normalize_string_list(
                    item.get("suggested_shared_terms")
                ),
                "suggested_actions": actions,
                "rationale": str(item.get("rationale", "")).strip(),
            }
        )

    return {
        "summary": str(data.get("summary", "")).strip(),
        "groups": groups,
        "ungrouped_keyword_ids": _normalize_string_list(
            data.get("ungrouped_keyword_ids")
        ),
        "global_recommendations": _normalize_string_list(
            data.get("global_recommendations")
        ),
    }


# ----------------------------
# ENRICHMENT PROMPT
# ----------------------------
KEYWORD_ENRICHMENT_PROMPT_TEMPLATE = """You are enriching keyword candidates for a call analytics system.

Your task is to improve rule-generated candidate phrases by:
1. Suggesting stronger, more descriptive labels
2. Suggesting appropriate categories
3. Suggesting 1-{max_aliases} alias phrases (synonyms, variations, common misspellings)
4. Identifying near-duplicate candidates that should be merged

Rules:
- Be conservative — only suggest aliases that are genuine synonyms or variations.
- Preserve all source evidence (support_calls, total_matches, sample_call_ids).
- When merging near-duplicates, keep the candidate with higher support_calls as primary.
- Categories should be from: billing, delivery, technical_support, complaint, inquiry, scheduling, pricing, quality, cancellation, generated.
- Return only JSON.

Return a JSON object with this structure:
{{
  "enriched_candidates": [
    {{
      "candidate_id": "original candidate_id",
      "phrase": "original phrase",
      "suggested_label": "stronger label or null",
      "suggested_category": "category or null",
      "suggested_aliases": ["alias1", "alias2"],
      "merged_with": [],
      "confidence_score": 0.85,
      "reason": "brief explanation"
    }}
  ],
  "merge_count": 0
}}

Candidates to enrich:
{candidates_json}
"""


# ----------------------------
# ALIAS EXPANSION PROMPT
# ----------------------------
KEYWORD_ALIAS_EXPANSION_PROMPT_TEMPLATE = """You are suggesting conservative alias phrases for an existing keyword in a call analytics system.

Keyword: {keyword_id}
Label: {label}
Current terms: {current_terms_json}
Recent matched texts (evidence):
{evidence_texts}

Your task: suggest up to {max_aliases} new alias phrases that would help this keyword match more relevant calls.

Rules:
- Only suggest genuine synonyms, variations, or common misspellings of existing terms.
- Do NOT invent entirely new concepts not related to current terms.
- Be conservative — better to suggest fewer high-quality aliases than many weak ones.
- Each alias should be a phrase that could appear in call analysis texts (summary, key_questions, objections).
- Return only JSON.

Return a JSON object with this structure:
{{
  "suggested_aliases": [
    {{
      "phrase": "alias phrase",
      "confidence_score": 0.9,
      "reason": "why this alias is relevant"
    }}
  ]
}}
"""


# ----------------------------
# DEEP INSIGHTS PROMPTS
# ----------------------------
PAIN_POINTS_PROMPT_TEMPLATE = """You are analyzing call transcripts to identify recurring customer pain points.

Analyze the following call analysis records and identify the top pain points customers experience.

Rules:
- Group related complaints into thematic pain points.
- Rate severity as low, medium, or high based on frequency and impact.
- Include evidence from actual call texts.
- Return only JSON.

Return a JSON object with this structure:
{{
  "insights": [
    {{
      "title": "short descriptive title",
      "description": "detailed explanation of the pain point",
      "severity": "low|medium|high",
      "affected_calls_count": 15,
      "evidence_summary": "brief summary of supporting evidence"
    }}
  ]
}}

Analysis records:
{analysis_records_json}
"""

OBJECTIONS_PROMPT_TEMPLATE = """You are analyzing call transcripts to identify and rank sales objections.

Analyze the following call analysis records and identify the most common objections raised by customers.

Rules:
- Cluster similar objections together.
- Rank by frequency and severity.
- Rate severity as low, medium, or high based on how often they block deals.
- Include evidence from actual call texts.
- Return only JSON.

Return a JSON object with this structure:
{{
  "insights": [
    {{
      "title": "objection category",
      "description": "detailed explanation of the objection pattern",
      "severity": "low|medium|high",
      "affected_calls_count": 12,
      "evidence_summary": "brief summary of supporting evidence"
    }}
  ]
}}

Analysis records:
{analysis_records_json}
"""

TRENDS_PROMPT_TEMPLATE = """You are analyzing call transcripts to identify trends over time.

Analyze the following call analysis records (which include timestamps) and identify evolving patterns in topics, intents, and outcomes.

Rules:
- Look for increasing or decreasing trends.
- Identify seasonal or temporal patterns.
- Note shifts in customer behavior or sentiment.
- Rate severity as low, medium, or high based on business impact.
- Return only JSON.

Return a JSON object with this structure:
{{
  "insights": [
    {{
      "title": "trend description",
      "description": "detailed explanation of the trend and its direction",
      "severity": "low|medium|high",
      "affected_calls_count": 25,
      "evidence_summary": "brief summary of supporting evidence"
    }}
  ]
}}

Analysis records:
{analysis_records_json}
"""

FOLLOW_UP_RISK_PROMPT_TEMPLATE = """You are analyzing call transcripts to identify calls/contacts at risk.

Analyze the following call analysis records and flag those that indicate follow-up risk (churn risk, unresolved issues, dissatisfied customers).

Rules:
- Look for signals of dissatisfaction, unresolved problems, or churn indicators.
- Rate severity as low, medium, or high based on urgency.
- Include evidence from actual call texts.
- Return only JSON.

Return a JSON object with this structure:
{{
  "insights": [
    {{
      "title": "risk category",
      "description": "detailed explanation of the risk and why it matters",
      "severity": "low|medium|high",
      "affected_calls_count": 8,
      "evidence_summary": "brief summary of supporting evidence"
    }}
  ]
}}

Analysis records:
{analysis_records_json}
"""


def ollama_enrich_candidates(
    candidates: List[Dict[str, Any]],
    config: AppConfig,
    max_aliases: int = 3,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    prompt = KEYWORD_ENRICHMENT_PROMPT_TEMPLATE.format(
        max_aliases=max_aliases,
        candidates_json=json.dumps(candidates, ensure_ascii=False, indent=2),
    )
    logger.info(
        "Sending enrichment request to Ollama: candidates=%d prompt_chars=%d",
        len(candidates),
        len(prompt),
    )
    raw = _ollama_generate(prompt, config, temperature=0.2, force_json=True)
    logger.info(
        "Received enrichment response from Ollama: response_chars=%d elapsed_s=%.2f",
        len(raw),
        time.perf_counter() - started_at,
    )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = _extract_json_object(raw)

    if not isinstance(data, dict):
        raise ValueError("Enrichment response must be a JSON object")

    enriched: List[Dict[str, Any]] = []
    for item in data.get("enriched_candidates", []):
        if not isinstance(item, dict):
            continue
        enriched.append(
            {
                "candidate_id": str(item.get("candidate_id", "")).strip(),
                "phrase": str(item.get("phrase", "")).strip(),
                "suggested_label": str(item.get("suggested_label") or "").strip()
                or None,
                "suggested_category": str(item.get("suggested_category") or "").strip()
                or None,
                "suggested_aliases": _normalize_string_list(
                    item.get("suggested_aliases")
                ),
                "merged_with": _normalize_string_list(item.get("merged_with")),
                "confidence_score": (
                    float(item["confidence_score"])
                    if item.get("confidence_score") is not None
                    else None
                ),
                "reason": str(item.get("reason", "")).strip() or None,
            }
        )

    return {
        "enriched_candidates": enriched,
        "merge_count": int(data.get("merge_count", 0)),
    }


def ollama_expand_aliases(
    keyword_id: str,
    label: str,
    current_terms: List[str],
    evidence_texts: List[str],
    max_aliases: int,
    config: AppConfig,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    prompt = KEYWORD_ALIAS_EXPANSION_PROMPT_TEMPLATE.format(
        keyword_id=keyword_id,
        label=label,
        current_terms_json=json.dumps(current_terms, ensure_ascii=False),
        evidence_texts="\n".join(f"- {t}" for t in evidence_texts[:50]),
        max_aliases=max_aliases,
    )
    logger.info(
        "Sending alias expansion request to Ollama: keyword=%s prompt_chars=%d",
        keyword_id,
        len(prompt),
    )
    raw = _ollama_generate(prompt, config, temperature=0.2, force_json=True)
    logger.info(
        "Received alias expansion response from Ollama: response_chars=%d elapsed_s=%.2f",
        len(raw),
        time.perf_counter() - started_at,
    )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = _extract_json_object(raw)

    if not isinstance(data, dict):
        raise ValueError("Alias expansion response must be a JSON object")

    suggestions: List[Dict[str, Any]] = []
    for item in data.get("suggested_aliases", []):
        if not isinstance(item, dict):
            continue
        phrase = str(item.get("phrase", "")).strip()
        if not phrase:
            continue
        suggestions.append(
            {
                "phrase": phrase,
                "confidence_score": (
                    float(item["confidence_score"])
                    if item.get("confidence_score") is not None
                    else None
                ),
                "reason": str(item.get("reason", "")).strip() or None,
            }
        )

    return {"suggested_aliases": suggestions[:max_aliases]}


def ollama_generate_deep_insights(
    insight_type: str,
    analysis_records: List[Dict[str, Any]],
    config: AppConfig,
) -> Dict[str, Any]:
    prompt_map = {
        "pain_points": PAIN_POINTS_PROMPT_TEMPLATE,
        "objections": OBJECTIONS_PROMPT_TEMPLATE,
        "trends": TRENDS_PROMPT_TEMPLATE,
        "follow_up_risk": FOLLOW_UP_RISK_PROMPT_TEMPLATE,
    }
    template = prompt_map.get(insight_type)
    if not template:
        raise ValueError(f"Unknown insight type: {insight_type}")

    started_at = time.perf_counter()
    # Records are already capped by the caller (_collect_analysis_records).
    prompt = template.format(
        analysis_records_json=json.dumps(
            analysis_records, ensure_ascii=False, indent=2
        ),
    )
    logger.info(
        "Sending deep insights request to Ollama: type=%s records=%d prompt_chars=%d",
        insight_type,
        len(analysis_records),
        len(prompt),
    )
    raw = _ollama_generate(prompt, config, temperature=0.3, force_json=True)
    logger.info(
        "Received deep insights response from Ollama: type=%s response_chars=%d elapsed_s=%.2f",
        insight_type,
        len(raw),
        time.perf_counter() - started_at,
    )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = _extract_json_object(raw)

    if not isinstance(data, dict):
        raise ValueError("Deep insights response must be a JSON object")

    insights: List[Dict[str, Any]] = []
    for item in data.get("insights", []):
        if not isinstance(item, dict):
            continue
        severity_raw = str(item.get("severity", "low")).strip().lower()
        if severity_raw not in ("low", "medium", "high"):
            severity_raw = "low"
        insights.append(
            {
                "title": str(item.get("title", "")).strip(),
                "description": str(item.get("description", "")).strip(),
                "severity": severity_raw,
                "affected_calls_count": int(item.get("affected_calls_count", 0)),
                "evidence_summary": str(item.get("evidence_summary", "")).strip()
                or None,
            }
        )

    return {"insights": insights}
