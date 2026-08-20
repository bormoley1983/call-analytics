"""Port for HTML report rendering.

``core.snapshot_export.export_snapshot_reports`` previously imported the
concrete ``adapters.reports_html`` renderers directly, which broke the
hexagonal rule that core may only depend on domain + ports. The adapter now
implements this Protocol and is injected by the composition root.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ReportRendererPort(Protocol):
    """Renders report data dicts to HTML files."""

    def render_overall_report(self, report: dict[str, Any], out_path: Path) -> None:
        """Render the overall (company-wide) report to ``out_path``."""
        ...

    def render_manager_report(self, report: dict[str, Any], out_path: Path) -> None:
        """Render the per-manager report to ``out_path``."""
        ...
