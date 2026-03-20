from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any


@dataclass(frozen=True)
class ReportCallRecord:
    call_id: str
    manager_id: str
    manager_name: str
    role: str
    direction: str
    spam_probability: float
    effective_call: bool
    intent: str
    outcome: str
    summary: str
    audio_seconds: float
    call_date: str
    key_questions: list[str] = field(default_factory=list)
    objections: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReportFilters:
    date_from: date | None = None
    date_to: date | None = None
    manager_id: str | None = None
    role: str | None = None
    direction: str | None = None
    intent: str | None = None
    outcome: str | None = None
    spam_only: bool = False
    effective_only: bool = False

    @property
    def call_date_from(self) -> str | None:
        if self.date_from is None:
            return None
        return self.date_from.strftime("%Y%m%d")

    @property
    def call_date_to(self) -> str | None:
        if self.date_to is None:
            return None
        return self.date_to.strftime("%Y%m%d")

    def matches_record(self, record: ReportCallRecord) -> bool:
        if self.call_date_from and not record.call_date:
            return False
        if self.call_date_to and not record.call_date:
            return False
        if self.call_date_from and record.call_date < self.call_date_from:
            return False
        if self.call_date_to and record.call_date > self.call_date_to:
            return False
        if self.manager_id and record.manager_id != self.manager_id:
            return False
        if self.role and record.role != self.role:
            return False
        if self.direction and record.direction != self.direction:
            return False
        if self.intent and record.intent != self.intent:
            return False
        if self.outcome and record.outcome != self.outcome:
            return False
        return True

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.date_from is not None:
            data["date_from"] = self.date_from.isoformat()
        if self.date_to is not None:
            data["date_to"] = self.date_to.isoformat()
        return data
