import json
import re

from fastapi import APIRouter, HTTPException

from domain.config import OUT

router = APIRouter(prefix="/reports", tags=["reports"])

_SAFE_ID = re.compile(r'^[\w\-]+$')

@router.get("/overall")
def overall_report():
    path = OUT / "report.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not generated yet")
    return json.loads(path.read_text())

@router.get("/manager/{manager_id}")
def manager_report(manager_id: str):
    if not _SAFE_ID.match(manager_id):
        raise HTTPException(status_code=400, detail="Invalid manager_id")
    path = OUT / "reports" / f"{manager_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Manager report not found")
    return json.loads(path.read_text())