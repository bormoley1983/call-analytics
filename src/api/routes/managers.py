from fastapi import APIRouter

from api.report_schemas import ManagerEntry
from domain.config import load_app_config

router = APIRouter(prefix="/managers", tags=["managers"])


def _get_mapper():
    # No lru_cache — the mapper is cheap to build and caching it here would
    # hide config reloads (e.g. after managers.yaml changes or env overrides).
    return load_app_config().manager_mapper

@router.get(
    "",
    summary="List configured managers",
    description=(
        "Returns manager definitions from app configuration (management + sales), "
        "including internal extensions and external lines."
    ),
    response_model=list[ManagerEntry],
    operation_id="managers_list",
)
def list_managers() -> list[ManagerEntry]:
    mapper = _get_mapper()
    managers: list[ManagerEntry] = []

    for mgr in mapper.management_dev.get("managers", []):
        managers.append(ManagerEntry(
            id=mgr["id"],
            name=mgr["name"],
            role=mgr.get("role", "management"),
            internal_extensions=[str(e) for e in mgr.get("internal_extensions", [])],
            external_lines=mgr.get("external_lines", []),
        ))

    for mgr in mapper.sales:
        managers.append(ManagerEntry(
            id=mgr["id"],
            name=mgr["name"],
            role="sales",
            internal_extensions=[str(e) for e in mgr.get("internal_extensions", [])],
            external_lines=mgr.get("external_lines", []),
        ))

    return managers
