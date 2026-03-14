import requests
from fastapi import APIRouter

from domain.config import OLLAMA_URL

router = APIRouter(tags=["health"])

@router.get("/health")
def health():
    ollama_ok = False
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        ollama_ok = r.status_code == 200
    except Exception:
        pass

    return {
        "status": "ok",
        "ollama": "up" if ollama_ok else "down",
        "ollama_url": OLLAMA_URL,
    }