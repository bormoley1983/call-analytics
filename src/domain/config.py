"""
Configuration management for call analytics.
Loads settings from environment variables and YAML files.
"""

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

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

    Uses python-dotenv for robust parsing (inline comments, escaped quotes,
    export prefix, etc.). Only sets values that are not already in the
    environment (overlay mode) so explicit exports take precedence.
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
        from dotenv import dotenv_values

        values = dotenv_values(env_path)
        if not values:
            return

        loaded: list[str] = []
        skipped: list[str] = []
        for key, value in values.items():
            if not key or key in _ENV_KEYS_SKIP:
                continue
            if value is None:
                continue
            if key in os.environ:
                skipped.append(key)
            else:
                os.environ[key] = value
                loaded.append(key)

        if loaded:
            logger.debug(
                "Loaded %d env default(s) from %s: %s",
                len(loaded),
                env_path,
                ", ".join(sorted(loaded)),
            )
        if skipped:
            logger.debug(
                "Skipped %d already-set env key(s) from %s: %s",
                len(skipped),
                env_path,
                ", ".join(sorted(skipped)),
            )
    except ImportError:
        logger.warning(
            "python-dotenv not installed — .env loading skipped. "
            "Install with: pip install python-dotenv"
        )
    except OSError as exc:
        logger.warning("Could not load env defaults from %s: %s", env_path, exc)


def ensure_env_loaded() -> None:
    """Idempotent .env overlay; call from entrypoints, never at import.

    Importing ``domain.config`` must have no side effects (no env mutation, no
    I/O). Entrypoints (``api/app.py`` lifespan, ``cli.py``, ``api/runner.py``,
    ``migrate_storage.py``, ``stt_compare.py``, ``stt_replay.py``) call this
    once at startup so that ``config/.env`` defaults are available before the
    first :func:`load_app_config` / getter call.
    """
    global _ENV_LOADED
    if not _ENV_LOADED:
        _load_env_defaults()
        _ENV_LOADED = True


_ENV_LOADED = False


# ----------------------------
# Paths (computed at call time, no import-time env capture)
# ----------------------------
def get_root() -> Path:
    """Project root. PROJECT_ROOT is process-stable (skipped by the .env
    overlay), so reading it per call is safe and side-effect free."""
    return Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))


def get_calls_raw() -> Path:
    return get_root() / "calls_raw"


def get_out() -> Path:
    return get_root() / "out"


def get_norm() -> Path:
    return get_out() / "normalized"


def get_trans() -> Path:
    return get_out() / "transcripts"


def get_analysis_dir() -> Path:
    return get_out() / "analysis"


def get_config_dir() -> Path:
    return get_root() / "config"


def get_managers_config() -> Path:
    return get_config_dir() / "managers.yaml"


def get_brands_config() -> Path:
    return get_config_dir() / "brands.yaml"


def get_analysis_yaml_config() -> Path:
    return get_config_dir() / "analysis.yaml"


def get_keywords_config() -> Path:
    return get_config_dir() / "keywords.yaml"


# ----------------------------
# Environment Variables (read at call time, not import time)
# ----------------------------
_STT_ALIASES = {
    "whisper": "faster-whisper",
    "faster_whisper": "faster-whisper",
    "faster-whisper": "faster-whisper",
    "canary": "canary",
}


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, "1" if default else "0") == "1"


def get_ollama_url() -> str:
    return _env_str("OLLAMA_URL", "http://localhost:11434").rstrip("/")


def get_ollama_model() -> str:
    return _env_str("OLLAMA_MODEL", "qwen3.5:27b")


def get_ollama_num_ctx() -> int:
    return _env_int("OLLAMA_NUM_CTX", 32768)


def get_ollama_keep_alive() -> str:
    return _env_str("OLLAMA_KEEP_ALIVE", "0s").strip() or "0s"


def get_ollama_think() -> bool:
    return os.getenv("OLLAMA_THINK", "0") == "1"


def get_ollama_generation_timeout() -> int:
    return _env_int("OLLAMA_GENERATION_TIMEOUT", 600)


def get_ollama_retry_attempts() -> int:
    return _env_int("OLLAMA_RETRY_ATTEMPTS", 4)


def get_ollama_token_overhead() -> int:
    return _env_int("OLLAMA_TOKEN_OVERHEAD", 3000)


def get_analysis_workers() -> int:
    return _env_int("ANALYSIS_WORKERS", 1)


def get_spam_probability_threshold() -> float:
    return _env_float("SPAM_PROBABILITY_THRESHOLD", 0.7)


def get_stt_engine() -> str:
    raw = os.getenv("STT_ENGINE", "faster-whisper").strip().lower()
    return _STT_ALIASES.get(raw, raw)


def get_whisper_model() -> str:
    return _env_str("WHISPER_MODEL", "large-v3-turbo")


def get_device() -> str:
    return _env_str("WHISPER_DEVICE", "cuda")


def get_compute_type() -> str:
    return _env_str("WHISPER_COMPUTE_TYPE", "float16")


def get_whisper_beam_size() -> int:
    return _env_int("WHISPER_BEAM_SIZE", 5)


def get_stt_language() -> str:
    return os.getenv("STT_LANGUAGE", "auto").strip().lower()


def get_canary_model_id() -> str:
    return _env_str("CANARY_MODEL_ID", "nvidia/canary-1b-v2")


def get_canary_model_revision() -> str:
    return _env_str("CANARY_MODEL_REVISION", "unknown")


def get_canary_device() -> str:
    return os.getenv("CANARY_DEVICE") or get_device()


def get_canary_compute_type() -> str:
    return _env_str("CANARY_COMPUTE_TYPE", "float16")


def get_canary_batch_size() -> int:
    return _env_int("CANARY_BATCH_SIZE", 1)


def get_canary_beam_size() -> int:
    return _env_int("CANARY_BEAM_SIZE", 1)


def get_canary_task() -> str:
    return _env_str("CANARY_TASK", "asr")


def get_canary_source_lang() -> str:
    return _env_str("CANARY_SOURCE_LANG", "auto")


def get_canary_target_lang() -> str:
    return _env_str("CANARY_TARGET_LANG", "auto")


def get_canary_return_hypotheses() -> bool:
    return os.getenv("CANARY_RETURN_HYPOTHESES", "1") == "1"


def get_min_bytes() -> int:
    return _env_int("MIN_BYTES", 20000)


def get_min_seconds() -> float:
    return _env_float("MIN_SECONDS", 1.0)


def get_max_segments_translate() -> int:
    return _env_int("MAX_SEGMENTS_TRANSLATE", 60)


def get_max_chars_translate() -> int:
    return _env_int("MAX_CHARS_TRANSLATE", 12000)


def get_max_chars_analyze() -> int:
    return _env_int("MAX_CHARS_ANALYZE", 9000)


def get_postgres_dsn() -> str | None:
    """Single sanctioned place that reads POSTGRES_DSN."""
    return os.getenv("POSTGRES_DSN")


def get_days_scope() -> str:
    """DAYS env var (comma-separated day scope); empty when unset.

    Read lazily so per-run overrides are honored (E8 removed the import-time
    capture in runner._configure_process_env).
    """
    return os.getenv("DAYS", "")


def get_enable_tqdm() -> bool:
    return os.getenv("ENABLE_TQDM", "1") == "1"


def get_auto_run_ai_keyword_analysis() -> bool:
    return os.getenv("AUTO_RUN_AI_KEYWORD_ANALYSIS", "1") != "0"


def get_ollama_rate_limit() -> int:
    """Maximum concurrent Ollama requests (0 disables rate limiting)."""
    raw = os.environ.get("OLLAMA_RATE_LIMIT")
    if raw is None:
        return 4
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid OLLAMA_RATE_LIMIT=%r (not an integer), using default 4", raw
        )
        return 4


def get_ollama_rate_interval() -> float:
    """Minimum seconds between Ollama request starts."""
    return float(os.getenv("OLLAMA_RATE_INTERVAL", "0.5"))


def get_pg_pool_min() -> int:
    raw = os.environ.get("PG_POOL_MIN")
    if raw is None:
        return 1
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid PG_POOL_MIN=%r (not an integer), using default 1", raw)
        return 1


def get_pg_pool_max() -> int:
    raw = os.environ.get("PG_POOL_MAX")
    if raw is None:
        return 10
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid PG_POOL_MAX=%r (not an integer), using default 10", raw
        )
        return 10


def get_postgres_connect_timeout() -> int:
    """Postgres connect timeout in seconds (minimum 1)."""
    raw_value = os.getenv("POSTGRES_CONNECT_TIMEOUT", "10").strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 10


@dataclass(frozen=True)
class PbxConfig:
    """Connection settings for the PBX SSH/SFTP downloader.

    Mirrors ``domain.pbx.PbxConfig`` (kept there as the canonical type); this
    copy lets the sanctioned config layer build it without importing adapters.
    """

    host: str
    port: int = 22
    username: str = "asterisk"
    password: str | None = None
    key_path: str | None = None
    known_hosts_path: str | None = None
    remote_dir: str = "/var/spool/asterisk/monitor"


def get_pbx_ssh_insecure_autoload_enabled() -> bool:
    """E5: opt-in escape hatch for first-time SSH connections without known_hosts."""
    return os.getenv("PBX_SSH_INSECURE_AUTOADD", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def get_pbx_config() -> PbxConfig:
    """Build a :class:`PbxConfig` from environment variables.

    Raises:
        RuntimeError: if ``PBX_HOST`` is not set (fail fast, clear message).
    """
    host = os.getenv("PBX_HOST")
    if not host:
        raise RuntimeError(
            "PBX_HOST environment variable is not set. "
            "Set it to the PBX server address before running sync."
        )

    port_raw = os.getenv("PBX_PORT", "22")
    try:
        port = int(port_raw)
    except ValueError:
        raise RuntimeError(f"Invalid PBX_PORT={port_raw!r} (not an integer)") from None

    return PbxConfig(
        host=host,
        port=port,
        username=os.getenv("PBX_USER", "asterisk"),
        password=os.getenv("PBX_PASSWORD"),
        key_path=os.getenv("PBX_KEY_PATH"),
        known_hosts_path=os.getenv("PBX_KNOWN_HOSTS_PATH"),
        remote_dir=os.getenv("PBX_REMOTE_DIR", "/var/spool/asterisk/monitor"),
    )


# ----------------------------
# Manager Mapping
# ----------------------------
class ManagerMapper:
    """Maps phone numbers to managers based on configuration."""

    def __init__(self, config_path: Path):
        self.management_dev: dict[str, Any] = {}
        self.sales: list[dict[str, Any]] = []
        self.default_manager: dict[str, str] = {
            "name": "Unknown/General",
            "id": "manager_unknown",
            "role": "unknown",
        }

        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                self.management_dev = config.get("management_dev", {})
                self.sales = config.get("sales", {}).get("managers", [])
                self.default_manager = config.get(
                    "default_manager", self.default_manager
                )
        else:
            logger.warning("Manager config not found at %s", config_path)

    def normalize_number(self, number: str) -> str:
        """Remove all non-digit characters from phone number."""
        return re.sub(r"[^\d]", "", number)

    def find_manager(
        self, src_number: str, dst_number: str, direction: str
    ) -> dict[str, str]:
        """Find manager based on phone numbers and call direction."""
        src_norm = self.normalize_number(src_number)
        dst_norm = self.normalize_number(dst_number)

        # Check management/dev managers by extension FIRST
        for mgr in self.management_dev.get("managers", []):
            internal_exts = [
                self.normalize_number(str(ext))
                for ext in mgr.get("internal_extensions", [])
            ]

            if (
                direction == "incoming"
                and dst_norm in internal_exts
                or direction == "outgoing"
                and src_norm in internal_exts
            ):
                return {
                    "name": mgr["name"],
                    "id": mgr["id"],
                    "role": mgr.get("role", "management"),
                }

        # Check management/dev shared external line
        mgmt_line = self.normalize_number(
            self.management_dev.get("shared_external_line", "")
        )

        if mgmt_line and (src_norm == mgmt_line or dst_norm == mgmt_line):
            return {
                "name": "Management (general)",
                "id": "management_general",
                "role": "management",
            }

        # Check sales team
        for pass_num in range(2):
            for sales_mgr in self.sales:
                internal_exts = [
                    self.normalize_number(str(ext))
                    for ext in sales_mgr.get("internal_extensions", [])
                ]
                external_lines = [
                    self.normalize_number(num)
                    for num in sales_mgr.get("external_lines", [])
                ]

                if direction == "incoming":
                    ext_match = dst_norm in internal_exts
                    line_match = dst_norm in external_lines
                elif direction == "outgoing":
                    ext_match = src_norm in internal_exts
                    line_match = src_norm in external_lines
                else:
                    ext_match = line_match = False

                if (pass_num == 0 and ext_match) or (pass_num == 1 and line_match):
                    return {
                        "name": sales_mgr["name"],
                        "id": sales_mgr["id"],
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
    # Rate-limiter settings, consumed by adapters.llm_ollama at construction.
    ollama_rate_limit: int
    ollama_rate_interval: float
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
    analysis_config: dict[str, Any]
    brand_corrections: dict[str, str]
    manager_mapper: "ManagerMapper"


def load_app_config() -> AppConfig:
    logger.info("Loading configuration")

    get_config_dir().mkdir(parents=True, exist_ok=True)

    # Ollama probe is cached per (url, model) so repeated loads in a long-
    # lived API process don't re-query the server on every call.
    try:
        detected_context_window = get_ollama_model_context_window()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        logger.warning("Could not query Ollama, using default context: %s", e)
        detected_context_window = 4096

    context_window = min(detected_context_window, get_ollama_num_ctx())

    analysis_config = load_analysis_config()
    brand_corrections, whisper_prompt = load_brand_corrections()
    manager_mapper = ManagerMapper(get_managers_config())

    stt_engine = get_stt_engine()
    if stt_engine not in {"faster-whisper", "canary"}:
        logger.warning(
            "Unsupported STT_ENGINE=%s, falling back to faster-whisper", stt_engine
        )
        stt_engine = "faster-whisper"
    stt_engine = cast(Literal["faster-whisper", "canary"], stt_engine)

    # Read mutable flags at runtime so API request overrides are honored
    process_limit = int(os.getenv("PROCESS_LIMIT", "30"))
    force_reanalyze = os.getenv("FORCE_REANALYZE", "0") == "1"
    force_retranscribe = os.getenv("FORCE_RETRANSCRIBE", "0") == "1"
    force_translate_uk = os.getenv("FORCE_TRANSLATE_UK", "0") == "1"

    analysis_workers = get_analysis_workers()
    spam_probability_threshold = get_spam_probability_threshold()

    logger.info(
        "Configuration loaded: model=%s context=%s tokens brand_corrections=%d "
        "managers=%d whisper=%s(%s/%s) limit=%d reanalyze=%s retranscribe=%s "
        "translate_uk=%s detected_ctx=%s keep_alive=%s think=%s "
        "stt_engine=%s canary_model=%s",
        get_ollama_model(),
        f"{context_window:,}",
        len(brand_corrections),
        len(manager_mapper.sales)
        + len(manager_mapper.management_dev.get("managers", [])),
        get_whisper_model(),
        get_device(),
        get_compute_type(),
        process_limit,
        force_reanalyze,
        force_retranscribe,
        force_translate_uk,
        f"{detected_context_window:,}",
        get_ollama_keep_alive(),
        get_ollama_think(),
        stt_engine,
        get_canary_model_id(),
    )

    return AppConfig(
        root=get_root(),
        calls_raw=get_calls_raw(),
        out=get_out(),
        norm=get_norm(),
        trans=get_trans(),
        analysis=get_analysis_dir(),
        config_dir=get_config_dir(),
        ollama_url=get_ollama_url(),
        ollama_model=get_ollama_model(),
        ollama_context_window=context_window,
        ollama_keep_alive=get_ollama_keep_alive(),
        ollama_think=get_ollama_think(),
        ollama_timeout=get_ollama_generation_timeout(),
        ollama_retries=get_ollama_retry_attempts(),
        ollama_token_overhead=get_ollama_token_overhead(),
        ollama_rate_limit=get_ollama_rate_limit(),
        ollama_rate_interval=get_ollama_rate_interval(),
        analysis_workers=analysis_workers,
        whisper_model=get_whisper_model(),
        whisper_device=get_device(),
        whisper_compute_type=get_compute_type(),
        whisper_beam_size=get_whisper_beam_size(),
        stt_language=get_stt_language(),
        whisper_initial_prompt=whisper_prompt,
        stt_engine=stt_engine,
        canary_model_id=get_canary_model_id(),
        canary_model_revision=get_canary_model_revision(),
        canary_device=get_canary_device(),
        canary_compute_type=get_canary_compute_type(),
        canary_batch_size=get_canary_batch_size(),
        canary_beam_size=get_canary_beam_size(),
        canary_task=get_canary_task(),
        canary_source_lang=get_canary_source_lang(),
        canary_target_lang=get_canary_target_lang(),
        canary_return_hypotheses=get_canary_return_hypotheses(),
        min_bytes=get_min_bytes(),
        min_seconds=get_min_seconds(),
        process_limit=process_limit,
        force_reanalyze=force_reanalyze,
        force_retranscribe=force_retranscribe,
        force_translate_uk=force_translate_uk,
        max_segments_translate=get_max_segments_translate(),
        max_chars_translate=get_max_chars_translate(),
        max_chars_analyze=get_max_chars_analyze(),
        spam_probability_threshold=spam_probability_threshold,
        analysis_config=analysis_config,
        brand_corrections=brand_corrections,
        manager_mapper=manager_mapper,
    )


# ----------------------------
# Config Loaders
# ----------------------------
# Cache the Ollama context-window probe per (url, model) so a long-lived
# API process doesn't re-query the server on every load_app_config() call.
_OLLAMA_CTX_CACHE: dict[tuple[str, str], int] = {}


def get_ollama_model_context_window() -> int:
    """
    Query Ollama API to get the model's context window size.
    Returns context window in tokens, or default 4096 if unable to determine.

    Results are cached per (url, model) for the process lifetime.
    """
    url = get_ollama_url()
    model = get_ollama_model()
    cache_key = (url, model)
    if cache_key in _OLLAMA_CTX_CACHE:
        return _OLLAMA_CTX_CACHE[cache_key]

    try:
        r = requests.post(
            f"{url}/api/show", json={"name": model}, timeout=10
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
            "context_length",
        ]

        for key in context_keys:
            if key in model_info:
                ctx = int(model_info[key])
                logger.info(
                    "Detected model context window: %s tokens (%s)", f"{ctx:,}", key
                )
                _OLLAMA_CTX_CACHE[cache_key] = ctx
                return ctx

        logger.warning("Context window not found in model_info, using default 4096")
        _OLLAMA_CTX_CACHE[cache_key] = 4096
        return 4096

    except requests.exceptions.ConnectionError:
        logger.error(
            "Cannot connect to Ollama at %s — make sure Ollama is running: 'ollama serve'",
            url,
        )
        _OLLAMA_CTX_CACHE[cache_key] = 4096
        return 4096
    except (requests.exceptions.Timeout, ValueError) as e:
        logger.warning("Could not query model info: %s", e)
        _OLLAMA_CTX_CACHE[cache_key] = 4096
        return 4096


def load_analysis_config() -> dict[str, Any]:
    """Load analysis configuration including company info and prompt template."""
    default_config = {
        "company": {
            "name": "Your Company",
            "business": "продукцію",
            "products": [],
            "brands": [],
        },
        "analysis_prompt": """
Ти аналізуєш телефонні дзвінки.

Транскрипт:
{transcript}

Поверни JSON з аналізом.
        """.strip(),
        "intents": [
            "консультація",
            "скарга",
            "оформлення замовлення",
            "запит інформації",
            "інше",
        ],
        "outcomes": [
            "продаж",
            "консультація",
            "відмова",
            "переведення на іншого",
            "невідомо",
        ],
    }

    analysis_config_path = get_analysis_yaml_config()
    if not analysis_config_path.exists():
        logger.warning(
            "Analysis config not found at %s, using defaults", analysis_config_path
        )
        return default_config

    try:
        with open(analysis_config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return config if config else default_config
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Could not load analysis config: %s", e)
        return default_config


def load_brand_corrections() -> tuple[dict[str, str], str]:
    """
    Load brand name corrections and initial prompt from config.
    Returns (corrections_dict, initial_prompt).
    """
    default_corrections = {
        "AAA": "AAA",
        "XXX-групп": "XXX Group",
    }
    default_prompt = "Розмова про продукцію компанії."

    brands_config_path = get_brands_config()
    if not brands_config_path.exists():
        logger.warning(
            "Brands config not found at %s, using defaults", brands_config_path
        )
        return default_corrections, default_prompt

    try:
        with open(brands_config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            corrections = config.get("corrections", default_corrections)
            prompt = config.get("initial_prompt", default_prompt)
            return corrections, prompt
    except (OSError, yaml.YAMLError) as e:
        logger.warning("Could not load brands config: %s", e)
        return default_corrections, default_prompt
