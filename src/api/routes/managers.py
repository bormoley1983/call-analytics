from functools import lru_cache

from fastapi import APIRouter

from domain.config import load_app_config

router = APIRouter(prefix="/managers", tags=["managers"])

@lru_cache(maxsize=1)
def _get_mapper():
    return load_app_config().manager_mapper

@router.get("")
def list_managers():
    mapper = _get_mapper()
    managers = []

    for mgr in mapper.management_dev.get("managers", []):
        managers.append({
            "id": mgr["id"],
            "name": mgr["name"],
            "role": mgr.get("role", "management"),
            "internal_extensions": [str(e) for e in mgr.get("internal_extensions", [])],
            "external_lines": mgr.get("external_lines", []),
        })

    for mgr in mapper.sales:
        managers.append({
            "id": mgr["id"],
            "name": mgr["name"],
            "role": "sales",
            "internal_extensions": [str(e) for e in mgr.get("internal_extensions", [])],
            "external_lines": mgr.get("external_lines", []),
        })

    return managers