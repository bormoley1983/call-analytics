"""Single source of truth for analyses-table filter clauses.

Previously the same WHERE-clause logic was duplicated in three places:

- ``reporting_postgres.ReportingPostgres._build_where_clauses`` (no prefix)
- ``keywords_postgres.KeywordsPostgres._analysis_filter_clauses`` (``a.`` prefix)
- inline clauses inside ``keywords_postgres.build_materialized_keywords_report``

All three now delegate to :func:`build_analysis_filter_clauses`, which takes an
optional ``table_prefix`` so the same logic works for both unprefixed queries
(``FROM analyses``) and aliased joins (``JOIN analyses a``).
"""

from __future__ import annotations

from domain.reporting import ReportFilters


def build_analysis_filter_clauses(
    filters: ReportFilters,
    spam_threshold: float,
    *,
    table_prefix: str = "",
    include_runtime_flags: bool = True,
) -> tuple[list[str], list[object]]:
    """Build WHERE clauses + params for filtering the ``analyses`` table.

    Args:
        filters: Report filter values (dates, manager, role, direction, ...).
        spam_threshold: Threshold used when ``filters.spam_only`` is set.
        table_prefix: Column prefix, e.g. ``"a."`` for aliased joins or ``""``.
        include_runtime_flags: When False, skip the runtime-only flags
            (``spam_only`` / ``effective_only``) — used by report CTEs that
            apply those filters at a different layer.

    Returns:
        ``(clauses, params)`` where clauses[0] is always ``"1=1"`` so callers
        can safely do ``" AND ".join(clauses)``.
    """
    p = table_prefix
    clauses = ["1=1"]
    params: list[object] = []

    if filters.date_from:
        clauses.append(f"{p}call_datetime::date >= %s")
        params.append(filters.date_from)
    if filters.date_to:
        clauses.append(f"{p}call_datetime::date <= %s")
        params.append(filters.date_to)
    if filters.manager_id:
        clauses.append(f"{p}manager_id = %s")
        params.append(filters.manager_id)
    if filters.role:
        clauses.append(f"{p}role = %s")
        params.append(filters.role)
    if filters.direction:
        clauses.append(f"{p}direction = %s")
        params.append(filters.direction)
    if filters.intent:
        clauses.append(f"{p}intent = %s")
        params.append(filters.intent)
    if filters.outcome:
        clauses.append(f"{p}outcome = %s")
        params.append(filters.outcome)
    if include_runtime_flags and filters.spam_only:
        clauses.append(f"COALESCE({p}spam_probability, 0.0) >= %s")
        params.append(spam_threshold)
    if include_runtime_flags and filters.effective_only:
        clauses.append(f"COALESCE({p}effective_call, false) IS TRUE")

    return clauses, params
