# -*- coding: utf-8 -*-
"""
Configuration management for call analytics.
Loads settings from environment variables and YAML files.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
import yaml


# ----------------------------
# Paths
# ----------------------------
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
CALLS_RAW = ROOT / "calls_raw"
OUT = ROOT / "out"
NORM = OUT / "normalized"
TRANS = OUT / "transcripts"
ANALYSIS = OUT / "analysis"
CONFIG_DIR = ROOT / "config"
MANAGERS_CONFIG = CONFIG_DIR / "managers.yaml"
BRANDS_CONFIG = CONFIG_DIR / "brands.yaml"
ANALYSIS_CONFIG = CONFIG_DIR / "analysis.yaml"


# ----------------------------
# Environment Variables
# ----------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:32b")

WHISPER_MODEL = os.getenv("WHISPER_MODEL", "large-v3-turbo")
DEVICE = os.getenv("WHISPER_DEVICE", "cuda")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "float16")
WHISPER_BEAM_SIZE = int(os.getenv("WHISPER_BEAM_SIZE", "5"))

MIN_BYTES = int(os.getenv("MIN_BYTES", "20000"))
MIN_SECONDS = float(os.getenv("MIN_SECONDS", "1.0"))
PROCESS_LIMIT = int(os.getenv("PROCESS_LIMIT", "30"))

FORCE_REANALYZE = os.getenv("FORCE_REANALYZE", "0") == "1"
FORCE_RETRANSCRIBE = os.getenv("FORCE_RETRANSCRIBE", "0") == "1"
FORCE_TRANSLATE_UK = os.getenv("FORCE_TRANSLATE_UK", "0") == "1"

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
            print(f"⚠ Warning: Manager config not found at {config_path}")
    
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
        for sales_mgr in self.sales:
            internal_exts = [str(ext) for ext in sales_mgr.get('internal_extensions', [])]
            external_lines = [
                self.normalize_number(num) 
                for num in sales_mgr.get('external_lines', [])
            ]
            
            if direction == "incoming":
                if dst_number in internal_exts or dst_norm in external_lines:
                    return {
                        "name": sales_mgr['name'],
                        "id": sales_mgr['id'],
                        "role": "sales"
                    }
            elif direction == "outgoing":
                if src_number in internal_exts or src_norm in external_lines:
                    return {
                        "name": sales_mgr['name'],
                        "id": sales_mgr['id'],
                        "role": "sales"
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
    ollama_timeout: int
    ollama_retries: int
    ollama_token_overhead: int 
    
    # Whisper settings
    whisper_model: str
    whisper_device: str
    whisper_compute_type: str
    whisper_beam_size: int
    whisper_initial_prompt: str
    
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
    """
    Load all configuration at startup.
    This is the main entry point for configuration loading.
    """
    print("\n" + "="*80)
    print("LOADING CONFIGURATION")
    print("="*80)
    
    # Ensure config directory exists
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load Ollama context (requires Ollama to be running)
    try:
        context_window = get_ollama_model_context_window()
    except Exception as e:
        print(f"⚠ Warning: Could not query Ollama, using default context: {e}")
        context_window = 4096
    
    # Load analysis config
    analysis_config = load_analysis_config()
    
    # Load brand corrections
    brand_corrections, whisper_prompt = load_brand_corrections()
    
    # Load manager mapper
    manager_mapper = ManagerMapper(MANAGERS_CONFIG)
    
    print(f"\n✓ Configuration loaded:")
    print(f"  - Ollama model: {OLLAMA_MODEL}")
    print(f"  - Context window: {context_window:,} tokens")
    print(f"  - Brand corrections: {len(brand_corrections)} entries")
    print(f"  - Managers configured: {len(manager_mapper.sales) + len(manager_mapper.management_dev.get('managers', []))}")
    print(f"  - Whisper model: {WHISPER_MODEL} ({DEVICE}/{COMPUTE_TYPE})")
    print(f"  - Processing limit: {PROCESS_LIMIT} files")
    print(f"  - Force reanalyze: {FORCE_REANALYZE}")
    print(f"  - Force retranscribe: {FORCE_RETRANSCRIBE}")
    print(f"  - Force translate UK: {FORCE_TRANSLATE_UK}")
    
    return AppConfig(
        # Paths
        root=ROOT,
        calls_raw=CALLS_RAW,
        out=OUT,
        norm=NORM,
        trans=TRANS,
        analysis=ANALYSIS,
        config_dir=CONFIG_DIR,
        
        # Ollama settings
        ollama_url=OLLAMA_URL,
        ollama_model=OLLAMA_MODEL,
        ollama_context_window=context_window,
        ollama_timeout=int(os.getenv("OLLAMA_GENERATION_TIMEOUT", "600")),
        ollama_retries=int(os.getenv("OLLAMA_RETRY_ATTEMPTS", "4")),
        ollama_token_overhead=int(os.getenv("OLLAMA_TOKEN_OVERHEAD", "1800")),
        
        # Whisper settings
        whisper_model=WHISPER_MODEL,
        whisper_device=DEVICE,
        whisper_compute_type=COMPUTE_TYPE,
        whisper_beam_size=WHISPER_BEAM_SIZE,
        whisper_initial_prompt=whisper_prompt,
        
        # Processing settings
        min_bytes=MIN_BYTES,
        min_seconds=MIN_SECONDS,
        process_limit=PROCESS_LIMIT,
        
        # Control flags
        force_reanalyze=FORCE_REANALYZE,
        force_retranscribe=FORCE_RETRANSCRIBE,
        force_translate_uk=FORCE_TRANSLATE_UK,
        
        # Translation limits
        max_segments_translate=MAX_SEGMENTS_TRANSLATE,
        max_chars_translate=MAX_CHARS_TRANSLATE,
        max_chars_analyze=MAX_CHARS_ANALYZE,
        
        # Thresholds
        spam_probability_threshold=float(os.getenv("SPAM_PROBABILITY_THRESHOLD", "0.7")),
        
        # Analysis configuration
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
            "qwen3moe.context_length",
            "qwen3.context_length",
            "qwen2.context_length",
            "llama.context_length",
            "mistral.context_length",
            "num_ctx",
            "context_length"
        ]
        
        for key in context_keys:
            if key in model_info:
                ctx = int(model_info[key])
                print(f"✓ Detected model context window: {ctx:,} tokens ({key})")
                return ctx
        
        print("Context window not found in model_info, using default 4096")
        return 4096
        
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot connect to Ollama at {OLLAMA_URL}")
        print("Make sure Ollama is running: 'ollama serve'")
        return 4096
    except Exception as e:
        print(f"Warning: Could not query model info: {e}")
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
        print(f"⚠ Analysis config not found at {ANALYSIS_CONFIG}, using defaults")
        return default_config
    
    try:
        with open(ANALYSIS_CONFIG, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config if config else default_config
    except Exception as e:
        print(f"⚠ Warning: Could not load analysis config: {e}")
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
        print(f"⚠ Brands config not found at {BRANDS_CONFIG}, using defaults")
        return default_corrections, default_prompt
    
    try:
        with open(BRANDS_CONFIG, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            corrections = config.get('corrections', default_corrections)
            prompt = config.get('initial_prompt', default_prompt)
            return corrections, prompt
    except Exception as e:
        print(f"⚠ Warning: Could not load brands config: {e}")
        return default_corrections, default_prompt
