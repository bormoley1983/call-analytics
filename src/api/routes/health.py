import logging
import time

import requests
from fastapi import APIRouter

from api.report_schemas import HealthResponse
from domain.config import get_ollama_url

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

# E4: cache the Ollama probe result briefly so a burst of /health requests does
# not fire a synchronous HTTP call per request. The TTL is short enough that a
# restart of Ollama is reflected within seconds.
_PROBE_TTL_SECONDS = 5.0
_probe_cache: dict[str, tuple[float, bool]] = {}


def _probe_ollama(ollama_url: str) -> bool:
    now = time.monotonic()
    cached = _probe_cache.get(ollama_url)
    if cached is not None and now - cached[0] < _PROBE_TTL_SECONDS:
        return cached[1]

    ok = False
    try:
        r = requests.get(f"{ollama_url}/api/tags", timeout=3)
        ok = r.status_code == 200
        if not ok:
            logger.warning("Ollama probe returned HTTP %s from %s", r.status_code, ollama_url)
    except requests.exceptions.RequestException as exc:
        # Log the failure instead of swallowing it silently — a down or
        # unreachable Ollama must be visible in the logs (the TTL cache above
        # keeps this from flooding on every /health request).
        logger.warning("Ollama probe failed for %s: %s", ollama_url, exc)

    _probe_cache[ollama_url] = (now, ok)
    return ok


@router.get("/health", response_model=HealthResponse, operation_id="health_get")
def health() -> HealthResponse:
    # Read the URL at request time, not import time.
    ollama_url = get_ollama_url()
    ollama_ok = _probe_ollama(ollama_url)

    return HealthResponse(
        status="ok",
        ollama="up" if ollama_ok else "down",
        ollama_url=ollama_url,
    )
