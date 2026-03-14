from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Dict, List


def _esc(v: Any) -> str:
    return html.escape(str(v))


def _pct(num: int, total: int) -> str:
    return f"{num / total * 100:.1f}%" if total else "0%"


def _fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def render_overall_report(report: Dict[str, Any], out_path: Path) -> None:
    total = report["total_calls"]
    rows = ""
    for label, key in [
        ("Total calls", "total_calls"),
        ("Transcribed", "transcribed"),
        ("Skipped (too small)", "skipped_too_small"),
        ("Skipped (too short)", "skipped_too_short"),
        ("Spam calls", "spam_calls"),
        ("Effective calls", "effective_calls"),
    ]:
        rows += f"<tr><td>{label}</td><td>{_esc(report.get(key, 0))}</td></tr>\n"

    duration = _fmt_duration(report.get("total_duration_seconds", 0))

    def top_table(items: List, title: str) -> str:
        if not items:
            return ""
        item_rows = "".join(f"<tr><td>{_esc(k)}</td><td>{_esc(v)}</td></tr>" for k, v in items)
        return f"<h3>{title}</h3><table><tr><th>Value</th><th>Count</th></tr>{item_rows}</table>"

    body = f"""
    <h1>Call Analytics — Overall Report</h1>
    <p>Generated: {_esc(report.get('generated_at', ''))}</p>
    <p>Total duration: {duration}</p>
    <table>
      <tr><th>Metric</th><th>Count</th></tr>
      {rows}
    </table>
    {top_table(report.get('top_intents', []), 'Top Intents')}
    {top_table(report.get('top_outcomes', []), 'Top Outcomes')}
    {top_table(report.get('top_questions', []), 'Top Questions')}
    """
    _write_html(out_path, "Overall Report", body)


def render_manager_report(report: Dict[str, Any], out_path: Path) -> None:
    sections = ""
    for role, managers in report.get("by_role", {}).items():
        cards = ""
        for m in managers:
            total = m["total_calls"]
            cards += f"""
            <div class="card">
              <h3>{_esc(m['manager_name'])} <small>({_esc(role)})</small></h3>
              <p>Calls: {total} — Effective: {m['effective_calls']} ({_pct(m['effective_calls'], total)})
                 — Spam: {m['spam_calls']} — Duration: {_fmt_duration(m['total_duration_seconds'])}</p>
              <p>Incoming: {m['incoming']} / Outgoing: {m['outgoing']}</p>
            </div>"""
        sections += f"<h2>Role: {_esc(role)}</h2>{cards}"

    body = f"""
    <h1>Call Analytics — Per-Manager Report</h1>
    <p>Generated: {_esc(report.get('generated_at', ''))}</p>
    <p>Total managers: {report.get('total_managers', 0)}</p>
    {sections}
    """
    _write_html(out_path, "Manager Report", body)


def _write_html(path: Path, title: str, body: str) -> None:
    css = """
    body { font-family: sans-serif; margin: 2em; }
    table { border-collapse: collapse; margin: 1em 0; }
    th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }
    th { background: #f4f4f4; }
    .card { border: 1px solid #ddd; border-radius: 6px; padding: 1em; margin: 0.5em 0; }
    h3 small { color: #888; font-weight: normal; }
    """
    out = f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(title)}</title><style>{css}</style></head><body>{body}</body></html>"
    path.write_text(out, encoding="utf-8")