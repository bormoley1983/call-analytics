from __future__ import annotations

from typing import Iterable

from adapters.postgres_single_connection import (
    RETRYABLE_CONNECTION_ERRORS, SingleConnectionPostgresAdapter)
from domain.reporting import ReportCallRecord, ReportFilters


class PostgresReportingSource(SingleConnectionPostgresAdapter):
    source_name = "postgres"

    @staticmethod
    def _normalize_phone_sql(column: str) -> str:
        digits = f"regexp_replace(COALESCE({column}, ''), '[^0-9]', '', 'g')"
        trimmed = f"CASE WHEN {digits} LIKE '00%' THEN substr({digits}, 3) ELSE {digits} END"
        return (
            "CASE "
            f"WHEN {trimmed} = '' THEN '' "
            f"WHEN length({trimmed}) = 10 AND {trimmed} LIKE '0%' THEN '38' || {trimmed} "
            f"ELSE {trimmed} "
            "END"
        )

    def _build_where_clauses(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        *,
        include_runtime_flags: bool = True,
    ) -> tuple[str, list[object]]:
        clauses = ["1=1"]
        params: list[object] = []

        if filters.call_date_from:
            clauses.append("call_date >= %s")
            params.append(filters.call_date_from)
        if filters.call_date_to:
            clauses.append("call_date <= %s")
            params.append(filters.call_date_to)
        if filters.manager_id:
            clauses.append("manager_id = %s")
            params.append(filters.manager_id)
        if filters.role:
            clauses.append("role = %s")
            params.append(filters.role)
        if filters.direction:
            clauses.append("direction = %s")
            params.append(filters.direction)
        if filters.intent:
            clauses.append("intent = %s")
            params.append(filters.intent)
        if filters.outcome:
            clauses.append("outcome = %s")
            params.append(filters.outcome)
        if include_runtime_flags and filters.spam_only:
            clauses.append("COALESCE(spam_probability, 0.0) >= %s")
            params.append(spam_threshold)
        if include_runtime_flags and filters.effective_only:
            clauses.append("COALESCE(effective_call, false) IS TRUE")

        return " AND ".join(clauses), params

    def _base_cte_sql(self, filters: ReportFilters, spam_threshold: float) -> tuple[str, list[object]]:
        where_clause, params = self._build_where_clauses(filters, spam_threshold)
        sql = f"""
            WITH base AS (
                SELECT
                    call_id,
                    COALESCE(manager_id, 'manager_unknown') AS manager_id,
                    COALESCE(manager_name, 'Unknown/General') AS manager_name,
                    COALESCE(role, 'unknown') AS role,
                    COALESCE(direction, 'unknown') AS direction,
                    COALESCE(spam_probability, 0.0)::double precision AS spam_probability,
                    COALESCE(effective_call, false) AS effective_call,
                    COALESCE(intent, 'інше') AS intent,
                    COALESCE(outcome, 'невідомо') AS outcome,
                    COALESCE(summary, '') AS summary,
                    COALESCE(audio_seconds, 0.0)::double precision AS audio_seconds,
                    COALESCE(call_date, '') AS call_date,
                    COALESCE(src_number, '') AS src_number,
                    COALESCE(dst_number, '') AS dst_number,
                    CASE
                        WHEN jsonb_typeof(key_questions) = 'array' THEN key_questions
                        ELSE '[]'::jsonb
                    END AS key_questions,
                    CASE
                        WHEN jsonb_typeof(objections) = 'array' THEN objections
                        ELSE '[]'::jsonb
                    END AS objections
                FROM analyses
                WHERE {where_clause}
            )
        """
        return sql, params

    def build_overall_report_data(self, filters: ReportFilters, spam_threshold: float) -> dict[str, object]:
        base_cte, base_params = self._base_cte_sql(filters, spam_threshold)

        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute(
                    base_cte
                    + """
                    SELECT
                        COUNT(*)::bigint AS total_calls,
                        COUNT(DISTINCT manager_id)::bigint AS unique_managers,
                        SUM(CASE WHEN spam_probability >= %s THEN 1 ELSE 0 END)::bigint AS spam_calls,
                        SUM(CASE WHEN effective_call THEN 1 ELSE 0 END)::bigint AS effective_calls,
                        COALESCE(SUM(audio_seconds), 0.0)::double precision AS total_duration_seconds
                    FROM base
                    """,
                    [*base_params, spam_threshold],
                )
                row = cur.fetchone()
                total_calls = int(row[0] or 0)
                unique_managers = int(row[1] or 0)
                spam_calls = int(row[2] or 0)
                effective_calls = int(row[3] or 0)
                total_duration = float(row[4] or 0.0)

                cur.execute(
                    base_cte
                    + """
                    SELECT intent, COUNT(*)::bigint AS cnt
                    FROM base
                    GROUP BY intent
                    ORDER BY cnt DESC, intent ASC
                    LIMIT 10
                    """,
                    base_params,
                )
                top_intents = [(str(item[0]), int(item[1] or 0)) for item in cur.fetchall()]

                cur.execute(
                    base_cte
                    + """
                    SELECT outcome, COUNT(*)::bigint AS cnt
                    FROM base
                    GROUP BY outcome
                    ORDER BY cnt DESC, outcome ASC
                    LIMIT 5
                    """,
                    base_params,
                )
                top_outcomes = [(str(item[0]), int(item[1] or 0)) for item in cur.fetchall()]

                cur.execute(
                    base_cte
                    + """
                    SELECT normalized_question, COUNT(*)::bigint AS cnt
                    FROM (
                        SELECT lower(btrim(q.value)) AS normalized_question
                        FROM base
                        CROSS JOIN LATERAL jsonb_array_elements_text(base.key_questions) AS q(value)
                    ) question_rows
                    WHERE normalized_question <> ''
                    GROUP BY normalized_question
                    ORDER BY cnt DESC, normalized_question ASC
                    LIMIT 10
                    """,
                    base_params,
                )
                top_questions = [(str(item[0]), int(item[1] or 0)) for item in cur.fetchall()]

            return {
                "total_calls": total_calls,
                "analyzed_calls": total_calls,
                "unique_managers": unique_managers,
                "spam_calls": spam_calls,
                "effective_calls": effective_calls,
                "total_duration_seconds": total_duration,
                "top_intents": top_intents,
                "top_outcomes": top_outcomes,
                "top_questions": top_questions,
            }

        return self._run_read(_fetch)

    def build_managers_report_data(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "total_calls",
        order: str = "desc",
    ) -> dict[str, object]:
        base_cte, base_params = self._base_cte_sql(filters, spam_threshold)

        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute(
                    base_cte
                    + """
                    SELECT
                        manager_id,
                        manager_name,
                        role,
                        COUNT(*)::bigint AS total_calls,
                        SUM(CASE WHEN direction = 'incoming' THEN 1 ELSE 0 END)::bigint AS incoming,
                        SUM(CASE WHEN direction = 'outgoing' THEN 1 ELSE 0 END)::bigint AS outgoing,
                        SUM(CASE WHEN spam_probability >= %s THEN 1 ELSE 0 END)::bigint AS spam_calls,
                        SUM(CASE WHEN effective_call THEN 1 ELSE 0 END)::bigint AS effective_calls,
                        COALESCE(SUM(audio_seconds), 0.0)::double precision AS total_duration_seconds
                    FROM base
                    GROUP BY manager_id, manager_name, role
                    ORDER BY role, manager_name, manager_id
                    """,
                    [*base_params, spam_threshold],
                )
                aggregate_rows = cur.fetchall()

                cur.execute(
                    base_cte
                    + """
                    SELECT role, COUNT(*)::bigint AS total_calls
                    FROM base
                    GROUP BY role
                    ORDER BY role
                    """,
                    base_params,
                )
                role_summary = {
                    str(role): {"total_calls": int(total_calls or 0)}
                    for role, total_calls in cur.fetchall()
                }

                cur.execute(
                    base_cte
                    + """
                    SELECT manager_id, intent, COUNT(*)::bigint AS cnt
                    FROM base
                    GROUP BY manager_id, intent
                    ORDER BY manager_id, cnt DESC, intent ASC
                    """,
                    base_params,
                )
                intent_rows = cur.fetchall()

                cur.execute(
                    base_cte
                    + """
                    SELECT manager_id, outcome, COUNT(*)::bigint AS cnt
                    FROM base
                    GROUP BY manager_id, outcome
                    ORDER BY manager_id, cnt DESC, outcome ASC
                    """,
                    base_params,
                )
                outcome_rows = cur.fetchall()

                cur.execute(
                    base_cte
                    + """
                    SELECT manager_id, normalized_question, COUNT(*)::bigint AS cnt
                    FROM (
                        SELECT
                            manager_id,
                            lower(btrim(q.value)) AS normalized_question
                        FROM base
                        CROSS JOIN LATERAL jsonb_array_elements_text(base.key_questions) AS q(value)
                    ) question_rows
                    WHERE normalized_question <> ''
                    GROUP BY manager_id, normalized_question
                    ORDER BY manager_id, cnt DESC, normalized_question ASC
                    """,
                    base_params,
                )
                question_rows = cur.fetchall()

            manager_map: dict[str, dict[str, object]] = {}
            for (
                manager_id,
                manager_name,
                role,
                total_calls,
                incoming,
                outgoing,
                spam_calls,
                effective_calls,
                total_duration,
            ) in aggregate_rows:
                manager_key = str(manager_id)
                manager_map[manager_key] = {
                    "manager_id": manager_key,
                    "manager_name": str(manager_name),
                    "role": str(role),
                    "total_calls": int(total_calls or 0),
                    "incoming": int(incoming or 0),
                    "outgoing": int(outgoing or 0),
                    "spam_calls": int(spam_calls or 0),
                    "effective_calls": int(effective_calls or 0),
                    "total_duration_seconds": float(total_duration or 0.0),
                    "top_intents": [],
                    "top_outcomes": [],
                    "top_questions": [],
                }

            for manager_id, intent, count in intent_rows:
                bucket = manager_map.get(str(manager_id))
                if not bucket:
                    continue
                top_intents = bucket["top_intents"]
                if len(top_intents) < 10:
                    top_intents.append((str(intent), int(count or 0)))

            for manager_id, outcome, count in outcome_rows:
                bucket = manager_map.get(str(manager_id))
                if not bucket:
                    continue
                top_outcomes = bucket["top_outcomes"]
                if len(top_outcomes) < 5:
                    top_outcomes.append((str(outcome), int(count or 0)))

            for manager_id, question, count in question_rows:
                bucket = manager_map.get(str(manager_id))
                if not bucket:
                    continue
                top_questions = bucket["top_questions"]
                if len(top_questions) < 10:
                    top_questions.append((str(question), int(count or 0)))

            all_managers = list(manager_map.values())
            reverse = order == "desc"
            if sort_by == "manager_name":
                all_managers.sort(
                    key=lambda item: (str(item["manager_name"]), str(item["manager_id"])),
                    reverse=reverse,
                )
            else:
                all_managers.sort(
                    key=lambda item: (
                        item.get(sort_by, 0),
                        str(item["manager_name"]),
                        str(item["manager_id"]),
                    ),
                    reverse=reverse,
                )

            by_role: dict[str, list[dict[str, object]]] = {}
            for manager in all_managers:
                by_role.setdefault(str(manager["role"]), []).append(manager)

            return {
                "role_summary": role_summary,
                "by_role": by_role,
                "all_managers": all_managers,
                "total_managers": len(all_managers),
            }

        return self._run_read(_fetch)

    def build_customers_report_data(
        self,
        filters: ReportFilters,
        spam_threshold: float,
        sort_by: str = "total_calls",
        order: str = "desc",
    ) -> dict[str, object]:
        base_cte, base_params = self._base_cte_sql(filters, spam_threshold)
        src_norm = self._normalize_phone_sql("src_number")
        dst_norm = self._normalize_phone_sql("dst_number")

        enriched_cte = (
            base_cte
            + f"""
            , enriched AS (
                SELECT
                    base.*,
                    {src_norm} AS src_norm,
                    {dst_norm} AS dst_norm,
                    CASE
                        WHEN direction = 'incoming' THEN
                            CASE
                                WHEN {src_norm} <> '' THEN {src_norm}
                                WHEN {dst_norm} <> '' THEN {dst_norm}
                                ELSE 'unknown'
                            END
                        WHEN direction = 'outgoing' THEN
                            CASE
                                WHEN {dst_norm} <> '' THEN {dst_norm}
                                WHEN {src_norm} <> '' THEN {src_norm}
                                ELSE 'unknown'
                            END
                        ELSE
                            CASE
                                WHEN {src_norm} = '' AND {dst_norm} = '' THEN 'unknown'
                                WHEN length({src_norm}) >= length({dst_norm}) THEN {src_norm}
                                ELSE {dst_norm}
                            END
                    END AS customer_phone,
                    CASE
                        WHEN direction = 'incoming' THEN
                            CASE
                                WHEN {src_norm} <> '' THEN COALESCE(NULLIF(src_number, ''), {src_norm})
                                WHEN {dst_norm} <> '' THEN COALESCE(NULLIF(dst_number, ''), {dst_norm})
                                ELSE 'unknown'
                            END
                        WHEN direction = 'outgoing' THEN
                            CASE
                                WHEN {dst_norm} <> '' THEN COALESCE(NULLIF(dst_number, ''), {dst_norm})
                                WHEN {src_norm} <> '' THEN COALESCE(NULLIF(src_number, ''), {src_norm})
                                ELSE 'unknown'
                            END
                        ELSE
                            CASE
                                WHEN {src_norm} = '' AND {dst_norm} = '' THEN 'unknown'
                                WHEN length({src_norm}) >= length({dst_norm}) THEN COALESCE(NULLIF(src_number, ''), {src_norm})
                                ELSE COALESCE(NULLIF(dst_number, ''), {dst_norm})
                            END
                    END AS display_phone
                FROM base
            )
            """
        )

        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute(
                    enriched_cte
                    + """
                    SELECT
                        customer_phone,
                        COUNT(*)::bigint AS total_calls,
                        SUM(CASE WHEN direction = 'incoming' THEN 1 ELSE 0 END)::bigint AS incoming,
                        SUM(CASE WHEN direction = 'outgoing' THEN 1 ELSE 0 END)::bigint AS outgoing,
                        SUM(CASE WHEN spam_probability >= %s THEN 1 ELSE 0 END)::bigint AS spam_calls,
                        SUM(CASE WHEN effective_call THEN 1 ELSE 0 END)::bigint AS effective_calls,
                        COALESCE(SUM(audio_seconds), 0.0)::double precision AS total_duration_seconds,
                        MIN(NULLIF(call_date, '')) AS first_call_date,
                        MAX(NULLIF(call_date, '')) AS last_call_date,
                        MAX(display_phone) AS display_phone
                    FROM enriched
                    GROUP BY customer_phone
                    ORDER BY customer_phone
                    """,
                    [*base_params, spam_threshold],
                )
                aggregate_rows = cur.fetchall()

                cur.execute(
                    enriched_cte
                    + """
                    SELECT
                        customer_phone,
                        manager_id,
                        manager_name,
                        role,
                        COUNT(*)::bigint AS calls
                    FROM enriched
                    GROUP BY customer_phone, manager_id, manager_name, role
                    ORDER BY customer_phone, calls DESC, manager_name ASC, manager_id ASC
                    """,
                    base_params,
                )
                manager_rows = cur.fetchall()

                cur.execute(
                    enriched_cte
                    + """
                    SELECT customer_phone, intent, COUNT(*)::bigint AS cnt
                    FROM enriched
                    GROUP BY customer_phone, intent
                    ORDER BY customer_phone, cnt DESC, intent ASC
                    """,
                    base_params,
                )
                intent_rows = cur.fetchall()

                cur.execute(
                    enriched_cte
                    + """
                    SELECT customer_phone, outcome, COUNT(*)::bigint AS cnt
                    FROM enriched
                    GROUP BY customer_phone, outcome
                    ORDER BY customer_phone, cnt DESC, outcome ASC
                    """,
                    base_params,
                )
                outcome_rows = cur.fetchall()

                cur.execute(
                    enriched_cte
                    + """
                    SELECT customer_phone, normalized_question, COUNT(*)::bigint AS cnt
                    FROM (
                        SELECT
                            customer_phone,
                            lower(btrim(q.value)) AS normalized_question
                        FROM enriched
                        CROSS JOIN LATERAL jsonb_array_elements_text(enriched.key_questions) AS q(value)
                    ) question_rows
                    WHERE normalized_question <> ''
                    GROUP BY customer_phone, normalized_question
                    ORDER BY customer_phone, cnt DESC, normalized_question ASC
                    """,
                    base_params,
                )
                question_rows = cur.fetchall()

            customer_map: dict[str, dict[str, object]] = {}
            for (
                customer_phone,
                total_calls,
                incoming,
                outgoing,
                spam_calls,
                effective_calls,
                total_duration,
                first_call_date,
                last_call_date,
                display_phone,
            ) in aggregate_rows:
                key = str(customer_phone)
                customer_map[key] = {
                    "customer_phone": key,
                    "display_phone": str(display_phone or key or "unknown"),
                    "total_calls": int(total_calls or 0),
                    "incoming": int(incoming or 0),
                    "outgoing": int(outgoing or 0),
                    "spam_calls": int(spam_calls or 0),
                    "effective_calls": int(effective_calls or 0),
                    "total_duration_seconds": float(total_duration or 0.0),
                    "first_call_date": str(first_call_date) if first_call_date else None,
                    "last_call_date": str(last_call_date) if last_call_date else None,
                    "top_intents": [],
                    "top_outcomes": [],
                    "top_questions": [],
                    "managers": [],
                }

            for customer_phone, manager_id, manager_name, role, calls in manager_rows:
                bucket = customer_map.get(str(customer_phone))
                if not bucket:
                    continue
                bucket["managers"].append(
                    {
                        "manager_id": str(manager_id),
                        "manager_name": str(manager_name),
                        "role": str(role),
                        "calls": int(calls or 0),
                    }
                )

            for customer_phone, intent, count in intent_rows:
                bucket = customer_map.get(str(customer_phone))
                if not bucket:
                    continue
                top_intents = bucket["top_intents"]
                if len(top_intents) < 10:
                    top_intents.append((str(intent), int(count or 0)))

            for customer_phone, outcome, count in outcome_rows:
                bucket = customer_map.get(str(customer_phone))
                if not bucket:
                    continue
                top_outcomes = bucket["top_outcomes"]
                if len(top_outcomes) < 5:
                    top_outcomes.append((str(outcome), int(count or 0)))

            for customer_phone, question, count in question_rows:
                bucket = customer_map.get(str(customer_phone))
                if not bucket:
                    continue
                top_questions = bucket["top_questions"]
                if len(top_questions) < 10:
                    top_questions.append((str(question), int(count or 0)))

            all_customers = [customer_map[key] for key in sorted(customer_map)]
            reverse = order == "desc"
            if sort_by in {"customer_phone", "display_phone", "first_call_date", "last_call_date"}:
                all_customers.sort(
                    key=lambda item: (str(item.get(sort_by) or ""), str(item["customer_phone"])),
                    reverse=reverse,
                )
            else:
                all_customers.sort(
                    key=lambda item: (
                        item.get(sort_by, 0),
                        str(item.get("last_call_date") or ""),
                        str(item["customer_phone"]),
                    ),
                    reverse=reverse,
                )

            return {
                "all_customers": all_customers,
                "total_customers": len(all_customers),
            }

        return self._run_read(_fetch)

    def iter_call_records(self, filters: ReportFilters) -> Iterable[ReportCallRecord]:
        where_clause, params = self._build_where_clauses(
            filters,
            spam_threshold=1.0,
            include_runtime_flags=False,
        )

        query = f"""
            SELECT
                call_id,
                manager_id,
                manager_name,
                role,
                direction,
                spam_probability,
                effective_call,
                intent,
                outcome,
                summary,
                audio_seconds,
                call_date,
                src_number,
                dst_number,
                key_questions,
                objections
            FROM analyses
            WHERE {where_clause}
            ORDER BY call_date NULLS LAST, call_id
        """

        rows_yielded = 0
        for attempt in range(2):
            try:
                conn = self._getconn()
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    while True:
                        rows = cur.fetchmany(1000)
                        if not rows:
                            return
                        for row in rows:
                            rows_yielded += 1
                            yield ReportCallRecord(
                                call_id=row[0],
                                manager_id=row[1] or "manager_unknown",
                                manager_name=row[2] or "Unknown/General",
                                role=row[3] or "unknown",
                                direction=row[4] or "unknown",
                                spam_probability=float(row[5] or 0.0),
                                effective_call=bool(row[6]),
                                intent=row[7] or "інше",
                                outcome=row[8] or "невідомо",
                                summary=row[9] or "",
                                audio_seconds=float(row[10] or 0.0),
                                call_date=row[11] or "",
                                src_number=row[12] or "",
                                dst_number=row[13] or "",
                                key_questions=list(row[14] or []),
                                objections=list(row[15] or []),
                            )
                return
            except RETRYABLE_CONNECTION_ERRORS:
                self._close_conn()
                if rows_yielded > 0 or attempt == 1:
                    raise
