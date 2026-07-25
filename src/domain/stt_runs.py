from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

SttRunPurpose = Literal["production", "replay", "benchmark", "legacy_import"]
SttRunStatus = Literal[
    "pending",
    "running",
    "completed",
    "completed_with_errors",
    "failed",
    "cancelled",
]
SttResultStatus = Literal["ok", "failed", "skipped"]


@dataclass(frozen=True)
class SttRunManifest:
    run_id: str
    name: str
    purpose: SttRunPurpose
    provider: str
    model_id: str
    model_revision: str
    config_hash: str
    config_data: dict[str, Any] = field(default_factory=dict)
    code_revision: str = ""
    dataset_manifest_hash: str = ""
    hardware_data: dict[str, Any] = field(default_factory=dict)
    status: SttRunStatus = "pending"
    total_calls: int = 0
    completed_calls: int = 0
    failed_calls: int = 0
    skipped_calls: int = 0
    total_audio_seconds: float = 0.0
    model_load_seconds: float = 0.0
    inference_seconds: float = 0.0
    wall_seconds: float = 0.0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class SttRunResultRecord:
    run_id: str
    call_id: str
    audio_sha256: str
    audio_seconds: float
    status: SttResultStatus
    raw_payload: dict[str, Any] = field(default_factory=dict)
    canonical_payload: dict[str, Any] = field(default_factory=dict)
    text_sha256: str = ""
    elapsed_seconds: float = 0.0
    rtf: float = 0.0
    peak_vram_mb: float | None = None
    batch_size: int | None = None
    retry_count: int = 0
    warnings: list[str] = field(default_factory=list)
    error_category: str | None = None
    error_detail: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
