# -*- coding: utf-8 -*-
"""
Configuration management for call analytics.
Loads settings from environment variables and YAML files.
"""

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple, cast

import requests
import yaml

logger = logging.getLogger(__name__)


# Keys that must never be loaded from .env because they are environment-specific.
# PROJECT_ROOT is auto-detected from the source tree; overwriting it with a
# Docker-only path (/work) breaks local development and tests.
_ENV_KEYS_SKIP = {"PROJECT_ROOT"}


def _load_env_defaults() -> None:
    """
    Load env defaults from config/.env when process env is missing keys.

    This keeps explicit exported variables authoritative and provides a safe
    fallback for launches that do not source env files (for example plain
    `uvicorn api.app:app` runs).
    """
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        repo_root / "config" / ".env",
        Path.cwd() / "config" / ".env",
        Path.cwd() / ".env",
    ]
    env_path = next((path for path in candidates if path.exists()), None)
    if env_path is None:
        return

    try:
        with env_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in _ENV_KEYS_SKIP:
                    continue

                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except OSError as exc:
        logger.warning("Could not load env defaults from %s: %s", env_path, exc)


_load_env_defaults()

# ----------------------------
# Paths
# ----------------------------
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
CALLS_RAW = ROOT / "calls_raw"
OUT = ROOT / "out"
NORM = OUT / "normalized"
TRANS = OUT / "transcripts"
ANALYSIS = OUT / "analysis"
CONFIG_DIR = ROOT / "config"
MANAGERS_CONFIG = CONFIG_DIR / "managers.yaml"
BRANDS_CONFIG = CONFIG_DIR / "brands.yaml"
ANALYSIS_CONFIG = CONFIG_DIR / "analysis.yaml"
KEYWORDS_CONFIG = CONFIG_DIR / "keywords.yaml"


# ----------------------------
# Environment Variables
# ----------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3.5:27b")
OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "16384"))
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "0s").strip() or "0s"
OLLAMA_THINK = os.getenv("OLLAMA_THINK", "0") == "1"
OLLAMA_GENERATION_TIMEOUT = int(os.getenv("OLLAMA_GENERATION_TIMEOUT", "600"))
OLLAMA_RETRY_ATTEMPTS = int(os.getenv("OLLAMA_RETRY_ATTEMPTS", "4"))
OLLAMA_TOKEN_OVERHEAD = int(os.getenv("OLLAMA_TOKEN_OVERHEAD", "1800"))
        
ANALYSIS_WORKERS = int(os.getenv("ANALYSIS_WORKERS", "1"))
SPAM_PROBABILITY_THRESHOLD = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))

_raw_stt_engine = os.getenv("STT_ENGINE", "faster-whisper").strip().lower()
_stt_aliases = {
    "whisper": "faster-whisper",
    "faster_whisper": "faster-whisper",
    "faster-whisper": "faster-whisper",
    "canary": "canary",
}
STT_ENGINE = _stt_aliases.get(_raw_stt_engine, _raw_stt_engine)

WHISPER_MODEL = os.getenv("WHISPER_MODEL", "large-v3-turbo")
DEVICE = os.getenv("WHISPER_DEVICE", "cuda")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "float16")
WHISPER_BEAM_SIZE = int(os.getenv("WHISPER_BEAM_SIZE", "5"))
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "auto").strip().lower()

CANARY_MODEL_ID = os.getenv("CANARY_MODEL_ID", "nvidia/canary-1b-v2")
CANARY_MODEL_REVISION = os.getenv("CANARY_MODEL_REVISION", "unknown")
CANARY_DEVICE = os.getenv("CANARY_DEVICE", DEVICE)
CANARY_COMPUTE_TYPE = os.getenv("CANARY_COMPUTE_TYPE", "float16")
CANARY_BATCH_SIZE = int(os.getenv("CANARY_BATCH_SIZE", "1"))
CANARY_BEAM_SIZE = int(os.getenv("CANARY_BEAM_SIZE", "1"))
CANARY_TASK = os.getenv("CANARY_TASK", "asr")
CANARY_SOURCE_LANG = os.getenv("CANARY_SOURCE_LANG", "auto")
CANARY_TARGET_LANG = os.getenv("CANARY_TARGET_LANG", "auto")
CANARY_RETURN_HYPOTHESES = os.getenv("CANARY_RETURN_HYPOTHESES", "1") == "1"

MIN_BYTES = int(os.getenv("MIN_BYTES", "20000"))
MIN_SECONDS = float(os.getenv("MIN_SECONDS", "1.0"))

MAX_SEGMENTS_TRANSLATE = int(os.getenv("MAX_SEGMENTS_TRANSLATE", "60"))
MAX_CHARS_TRANSLATE = int(os.getenv("MAX_CHARS_TRANSLATE", "12000"))
MAX_CHARS_ANALYZE = int(os.getenv("MAX_CHARS_ANALYZE", "9000"))


# ----------------------------
# Manager Mapping
# ----------------------------
class ManagerMapper:
    """Maps phone numbers to managers based on configuration."""
    
    def __init__(self, config_path: Path):
        self.management_dev: Dict[str, Any] = {}
        self.sales: List[Dict[str, Any]] = []
        self.default_manager: Dict[str, str] = {
            "name": "Unknown/General",
            "id": "manager_unknown",
            "role": "unknown"
        }
        
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self.management_dev = config.get('management_dev', {})
                self.sales = config.get('sales', {}).get('managers', [])
                self.default_manager = config.get('default_manager', self.default_manager)
        else:
            logger.warning("Manager config not found at %s", config_path)
    
    def normalize_number(self, number: str) -> str:
        """Remove all non-digit characters from phone number."""
        return re.sub(r'[^\d]', '', number)
    
    def find_manager(self, src_number: str, dst_number: str, direction: str) -> Dict[str, str]:
        """Find manager based on phone numbers and call direction."""
        src_norm = self.normalize_number(src_number)
        dst_norm = self.normalize_number(dst_number)
        
        # Check management/dev managers by extension FIRST
        for mgr in self.management_dev.get('managers', []):
            internal_exts = [str(ext) for ext in mgr.get('internal_extensions', [])]
            
            if direction == "incoming" and dst_number in internal_exts:
                return {
                    "name": mgr['name'],
                    "id": mgr['id'],
                    "role": mgr.get('role', 'management')
                }
            elif direction == "outgoing" and src_number in internal_exts:
                return {
                    "name": mgr['name'],
                    "id": mgr['id'],
                    "role": mgr.get('role', 'management')
                }
        
        # Check management/dev shared external line
        mgmt_line = self.normalize_number(
            self.management_dev.get('shared_external_line', '')
        )
        
        if mgmt_line and (src_norm == mgmt_line or dst_norm == mgmt_line):
            return {
                "name": "Management (general)",
                "id": "management_general",
                "role": "management"
            }
        
        # Check sales team
        for pass_num in range(2):
            for sales_mgr in self.sales:
                internal_exts = [str(ext) for ext in sales_mgr.get('internal_extensions', [])]
                external_lines = [
                    self.normalize_number(num) 
                    for num in sales_mgr.get('external_lines', [])
                ]
                
                if direction == "incoming":
                    ext_match = dst_number in internal_exts
                    line_match = dst_norm in external_lines
                elif direction == "outgoing":
                    ext_match = src_number in internal_exts
                    line_match = src_norm in external_lines
                else:
                    ext_match = line_match = False

                if (pass_num == 0 and ext_match) or (pass_num == 1 and line_match):
                    return {
                        "name": sales_mgr['name'],
                        "id": sales_mgr['id'],
                        "role": "sales",
                }
            
        return self.default_manager


# ----------------------------
# Configuration Dataclass
# ----------------------------
@dataclass
class AppConfig:
    """Application configuration loaded at runtime."""
    # Paths
    root: Path
    calls_raw: Path
    out: Path
    norm: Path
    trans: Path
    analysis: Path
    config_dir: Path
    
    # Ollama settings
    ollama_url: str
    ollama_model: str
    ollama_context_window: int
    ollama_keep_alive: str
    ollama_think: bool
    ollama_timeout: int
    ollama_retries: int
    ollama_token_overhead: int 
    analysis_workers: int
    
    # Whisper settings
    whisper_model: str
    whisper_device: str
    whisper_compute_type: str
    whisper_beam_size: int
    stt_language: str
    whisper_initial_prompt: str

    # STT engine selector and provider-specific settings
    stt_engine: Literal["faster-whisper", "canary"]
    canary_model_id: str
    canary_model_revision: str
    canary_device: str
    canary_compute_type: str
    canary_batch_size: int
    canary_beam_size: int
    canary_task: str
    canary_source_lang: str
    canary_target_lang: str
    canary_return_hypotheses: bool
    
    # Processing settings
    min_bytes: int
    min_seconds: float
    process_limit: int
    
    # Control flags
    force_reanalyze: bool
    force_retranscribe: bool
    force_translate_uk: bool
    
    # Translation limits
    max_segments_translate: int
    max_chars_translate: int
    max_chars_analyze: int
    
    # Thresholds
    spam_probability_threshold: float
    
    # Analysis configuration
    analysis_config: Dict[str, Any]
    brand_corrections: Dict[str, str]
    manager_mapper: 'ManagerMapper'


def load_app_config() -> AppConfig:
    logger.info("Loading configuration")

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        detected_context_window = get_ollama_model_context_window()
    except Exception as e:
        logger.warning("Could not query Ollama, using default context: %s", e)
        detected_context_window = 4096

    context_window = min(detected_context_window, OLLAMA_NUM_CTX)
    
    analysis_config = load_analysis_config()
    brand_corrections, whisper_prompt = load_brand_corrections()
    manager_mapper = ManagerMapper(MANAGERS_CONFIG)

    stt_engine = STT_ENGINE
    if stt_engine not in {"faster-whisper", "canary"}:
        logger.warning("Unsupported STT_ENGINE=%s, falling back to faster-whisper", stt_engine)
        stt_engine = "faster-whisper"
    stt_engine = cast(Literal["faster-whisper", "canary"], stt_engine)

    # Read mutable flags at runtime so API request overrides are honored
    process_limit = int(os.getenv("PROCESS_LIMIT", "30"))
    force_reanalyze = os.getenv("FORCE_REANALYZE", "0") == "1"
    force_retranscribe = os.getenv("FORCE_RETRANSCRIBE", "0") == "1"
    force_translate_uk = os.getenv("FORCE_TRANSLATE_UK", "0") == "1"

    analysis_workers = int(os.getenv("ANALYSIS_WORKERS", "1"))
    spam_probability_threshold = float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7"))
    
    logger.info(
        "Configuration loaded: model=%s context=%s tokens brand_corrections=%d "
        "managers=%d whisper=%s(%s/%s) limit=%d reanalyze=%s retranscribe=%s "
        "translate_uk=%s detected_ctx=%s keep_alive=%s think=%s "
        "stt_engine=%s canary_model=%s",
        OLLAMA_MODEL,
        f"{context_window:,}",
        len(brand_corrections),
        len(manager_mapper.sales) + len(manager_mapper.management_dev.get("managers", [])),
        WHISPER_MODEL, DEVICE, COMPUTE_TYPE,
        process_limit,
        force_reanalyze,
        force_retranscribe,
        force_translate_uk,
        f"{detected_context_window:,}",
        OLLAMA_KEEP_ALIVE,
        OLLAMA_THINK,
        stt_engine,
        CANARY_MODEL_ID,
    )
    
    return AppConfig(
        root=ROOT,
        calls_raw=CALLS_RAW,
        out=OUT,
        norm=NORM,
        trans=TRANS,
        analysis=ANALYSIS,
        config_dir=CONFIG_DIR,
        ollama_url=OLLAMA_URL,
        ollama_model=OLLAMA_MODEL,
        ollama_context_window=context_window,
        ollama_keep_alive=OLLAMA_KEEP_ALIVE,
        ollama_think=OLLAMA_THINK,
        ollama_timeout=OLLAMA_GENERATION_TIMEOUT,
        ollama_retries=OLLAMA_RETRY_ATTEMPTS,
        ollama_token_overhead=OLLAMA_TOKEN_OVERHEAD,
        analysis_workers=analysis_workers,
        whisper_model=WHISPER_MODEL,
        whisper_device=DEVICE,
        whisper_compute_type=COMPUTE_TYPE,
        whisper_beam_size=WHISPER_BEAM_SIZE,
        stt_language=STT_LANGUAGE,
        whisper_initial_prompt=whisper_prompt,
        stt_engine=stt_engine,
        canary_model_id=CANARY_MODEL_ID,
        canary_model_revision=CANARY_MODEL_REVISION,
        canary_device=CANARY_DEVICE,
        canary_compute_type=CANARY_COMPUTE_TYPE,
        canary_batch_size=CANARY_BATCH_SIZE,
        canary_beam_size=CANARY_BEAM_SIZE,
        canary_task=CANARY_TASK,
        canary_source_lang=CANARY_SOURCE_LANG,
        canary_target_lang=CANARY_TARGET_LANG,
        canary_return_hypotheses=CANARY_RETURN_HYPOTHESES,
        min_bytes=MIN_BYTES,
        min_seconds=MIN_SECONDS,
        process_limit=process_limit,
        force_reanalyze=force_reanalyze,
        force_retranscribe=force_retranscribe,
        force_translate_uk=force_translate_uk,
        max_segments_translate=MAX_SEGMENTS_TRANSLATE,
        max_chars_translate=MAX_CHARS_TRANSLATE,
        max_chars_analyze=MAX_CHARS_ANALYZE,
        spam_probability_threshold=spam_probability_threshold,
        analysis_config=analysis_config,
        brand_corrections=brand_corrections,
        manager_mapper=manager_mapper,
    )


# ----------------------------
# Config Loaders
# ----------------------------
def get_ollama_model_context_window() -> int:
    """
    Query Ollama API to get the model's context window size.
    Returns context window in tokens, or default 4096 if unable to determine.
    """
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/show",
            json={"name": OLLAMA_MODEL},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        
        model_info = data.get("model_info", {})
        
        context_keys = [
            "qwen35.context_length", 
            "qwen3.context_length",
            "qwen25.context_length",
            "qwen2.context_length",
            "llama.context_length",
            "num_ctx",
            "context_length"
        ]
        
        for key in context_keys:
            if key in model_info:
                ctx = int(model_info[key])
                logger.info("Detected model context window: %s tokens (%s)", f"{ctx:,}", key)
                return ctx

        logger.warning("Context window not found in model_info, using default 4096")
        return 4096

    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to Ollama at %s — make sure Ollama is running: 'ollama serve'", OLLAMA_URL)
        return 4096
    except Exception as e:
        logger.warning("Could not query model info: %s", e)
        return 4096


def load_analysis_config() -> Dict[str, Any]:
    """Load analysis configuration including company info and prompt template."""
    default_config = {
        "company": {
            "name": "Your Company",
            "business": "продукцію",
            "products": [],
            "brands": []
        },
        "analysis_prompt": """
Ти аналізуєш телефонні дзвінки.

Транскрипт:
{transcript}

Поверни JSON з аналізом.
        """.strip(),
        "intents": ["консультація", "скарга", "оформлення замовлення", "запит інформації", "інше"],
        "outcomes": ["продаж", "консультація", "відмова", "переведення на іншого", "невідомо"]
    }
    
    if not ANALYSIS_CONFIG.exists():
        logger.warning("Analysis config not found at %s, using defaults", ANALYSIS_CONFIG)
        return default_config

    try:
        with open(ANALYSIS_CONFIG, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config if config else default_config
    except Exception as e:
        logger.warning("Could not load analysis config: %s", e)
        return default_config


def load_brand_corrections() -> Tuple[Dict[str, str], str]:
    """
    Load brand name corrections and initial prompt from config.
    Returns (corrections_dict, initial_prompt).
    """
    default_corrections = {
        "AAA": "AAA",
        "XXX-групп": "XXX Group",
    }
    default_prompt = "Розмова про продукцію компанії."
    
    if not BRANDS_CONFIG.exists():
        logger.warning("Brands config not found at %s, using defaults", BRANDS_CONFIG)
        return default_corrections, default_prompt

    try:
        with open(BRANDS_CONFIG, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            corrections = config.get('corrections', default_corrections)
            prompt = config.get('initial_prompt', default_prompt)
            return corrections, prompt
    except Exception as e:
        logger.warning("Could not load brands config: %s", e)
        return default_corrections, default_prompt
