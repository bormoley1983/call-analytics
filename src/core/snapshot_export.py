from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from adapters.reports_html import render_manager_report, render_overall_report
from core.reporting_service import build_managers_report, build_overall_report
from domain.reporting import ReportFilters
from ports.reporting import ReportingSource


def export_snapshot_reports(
    *,
    output_dir: Path,
    source: ReportingSource,
    spam_threshold: float,
) -> dict[str, Any]:
    """Build and write report snapshots from persisted reporting source data."""
    filters = ReportFilters()
    overall = build_overall_report(source, filters, spam_threshold)
    by_manager = build_managers_report(source, filters, spam_threshold)

    report_json_path = output_dir / "report.json"
    report_by_manager_json_path = output_dir / "report_by_manager.json"
    report_html_path = output_dir / "report.html"
    report_by_manager_html_path = output_dir / "report_by_manager.html"

    report_json_path.write_text(
        json.dumps(overall, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_by_manager_json_path.write_text(
        json.dumps(by_manager, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    render_overall_report(overall, report_html_path)
    render_manager_report(by_manager, report_by_manager_html_path)

    return {
        "overall_report": str(report_json_path),
        "manager_report": str(report_by_manager_json_path),
        "overall_report_html": str(report_html_path),
        "manager_report_html": str(report_by_manager_html_path),
        "data_source": source.source_name,
        "filters": filters.as_dict(),
    }
