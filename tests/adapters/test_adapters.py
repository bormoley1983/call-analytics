from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast

import pytest

from adapters import (
    audio_ffmpeg,
    keyword_ai_analysis_postgres,
    keywords_postgres,
    llm_ollama,
    postgres_single_connection,
    reporting_postgres,
    storage_json,
    storage_postgres,
    storage_qdrant,
)
from domain.config import AppConfig
from domain.reporting import ReportFilters


def test_ffprobe_duration_seconds_exists():
    assert hasattr(audio_ffmpeg, "ffprobe_duration_seconds")


def test_json_storage_init(tmp_path):
    storage = storage_json.JsonStorage(tmp_path, tmp_path, tmp_path, tmp_path)
    assert storage.out == tmp_path


def test_json_storage_upsert_call_metadata_creates_and_updates_file(tmp_path):
    storage = storage_json.JsonStorage(
        tmp_path / "out", tmp_path / "norm", tmp_path / "trans", tmp_path / "analysis"
    )
    storage.ensure_ready()

    storage.upsert_call_metadata(
        call_id="call-1",
        source_file="a.wav",
        source_path="/tmp/a.wav",
        call_datetime=datetime(2026, 7, 25, tzinfo=timezone.utc),
        status="discovered",
    )
    storage.upsert_call_metadata(
        call_id="call-1",
        status="processed",
        error_message="analysis_failed",
        mark_synced=True,
    )

    payload = storage.load_call_metadata("call-1")
    assert payload["call_id"] == "call-1"
    assert payload["source_file"] == "a.wav"
    assert payload["status"] == "processed"
    assert payload["error_message"] == "analysis_failed"
    assert payload["discovered_at"]
    assert payload["transcribed_at"]
    assert payload["translated_at"]
    assert payload["analyzed_at"]
    assert payload["synced_at"]


def test_json_storage_save_transcript_and_analysis_updates_call_metadata(tmp_path):
    storage = storage_json.JsonStorage(
        tmp_path / "out", tmp_path / "norm", tmp_path / "trans", tmp_path / "analysis"
    )
    storage.ensure_ready()

    storage.save_transcript(
        "call-2",
        {
            "_pipeline_stage": "translated",
            "call_meta": {"date": "20260725"},
        },
    )
    storage.save_analysis(
        "call-2",
        {
            "source_file": "b.wav",
            "source_path": "/tmp/b.wav",
            "analysis_error": "",
            "call_meta": {"date": "20260725"},
        },
    )

    payload = storage.load_call_metadata("call-2")
    assert payload["call_id"] == "call-2"
    assert payload["status"] == "processed"
    assert payload["source_file"] == "b.wav"
    assert payload["call_datetime"] == "2026-07-25T00:00:00+00:00"
    assert payload["translated_at"]
    assert payload["analyzed_at"]


def test_json_storage_sync_per_call_tracks_metadata_and_saves_processed_analysis(
    tmp_path,
):
    storage = storage_json.JsonStorage(
        tmp_path / "out", tmp_path / "norm", tmp_path / "trans", tmp_path / "analysis"
    )
    storage.ensure_ready()

    per_call = [
        {
            "status": "processed",
            "meta": {
                "call_id": "call-10",
                "source_file": "a.wav",
                "source_path": "/tmp/a.wav",
                "date": "20260725",
            },
            "analysis": {"intent": "sale", "call_meta": {"date": "20260725"}},
        },
        {
            "status": "skipped_too_short",
            "meta": {
                "call_id": "call-11",
                "source_file": "b.wav",
                "source_path": "/tmp/b.wav",
                "date": "20260725",
            },
        },
    ]

    storage.sync_per_call(per_call)

    processed_meta = storage.load_call_metadata("call-10")
    skipped_meta = storage.load_call_metadata("call-11")
    processed_analysis = storage.load_analysis("call-10")

    assert processed_meta["status"] == "processed"
    assert processed_meta["source_file"] == "a.wav"
    assert processed_meta["analyzed_at"]
    assert skipped_meta["status"] == "skipped_too_short"
    assert skipped_meta["error_message"] == "duration_below_min_seconds"
    assert processed_analysis["intent"] == "sale"
    assert not storage.analysis_exists("call-11")


def test_postgres_jsonb_keeps_utf8_text():
    payload = {"text": "Привіт"}

    dumped = storage_postgres._jsonb(payload).dumps(payload)

    assert "Привіт" in dumped


def test_postgres_storage_forces_utf8_client_encoding():
    class DummyConn:
        def __init__(self):
            self.encoding = "SQLASCII"
            self.calls = []

        def set_client_encoding(self, value):
            self.calls.append(value)
            self.encoding = value

    conn = DummyConn()

    result = storage_postgres._ensure_utf8_client_encoding(conn)

    assert result is conn
    assert conn.calls == ["UTF8"]
    assert conn.encoding == "UTF8"


def test_postgres_storage_ddl_includes_calls_metadata_table_and_indexes():
    ddl = storage_postgres.DDL

    assert "CREATE TABLE IF NOT EXISTS calls" in ddl
    assert "source_file" in ddl
    assert "source_path" in ddl
    assert "status" in ddl
    assert "error_message" in ddl
    assert "idx_calls_status" in ddl
    assert "idx_calls_call_datetime" in ddl


def test_postgres_storage_sync_per_call_tracks_calls_metadata_for_processed_and_skipped():
    tracked_meta = []
    tracked_analysis = []

    class DummyStorage(storage_postgres.PostgresStorage):
        def __init__(self):
            pass

        def upsert_call_metadata(self, **kwargs):
            tracked_meta.append(kwargs)

        def upsert_analysis(self, call_id, data):
            tracked_analysis.append((call_id, data))

    storage = DummyStorage()

    per_call = [
        {
            "status": "processed",
            "meta": {
                "call_id": "call-1",
                "source_file": "a.wav",
                "source_path": "/tmp/a.wav",
                "date": "20260725",
            },
            "analysis": {"intent": "sale"},
        },
        {
            "status": "skipped_too_short",
            "meta": {
                "call_id": "call-2",
                "source_file": "b.wav",
                "source_path": "/tmp/b.wav",
                "date": "20260725",
            },
        },
    ]

    storage.sync_per_call(per_call)

    assert len(tracked_meta) == 2
    assert tracked_meta[0]["call_id"] == "call-1"
    assert tracked_meta[0]["status"] == "processed"
    assert tracked_meta[1]["call_id"] == "call-2"
    assert tracked_meta[1]["status"] == "skipped_too_short"
    assert tracked_meta[1]["error_message"] == "duration_below_min_seconds"
    assert tracked_analysis == [("call-1", {"intent": "sale"})]


def test_single_connection_adapter_adds_default_connect_timeout(monkeypatch):
    captured = {}

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return []

    class DummyConn:
        def __init__(self):
            self.encoding = "UTF8"
            self.closed = 0

        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

        def close(self):
            self.closed = 1

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "7")
    monkeypatch.setattr(
        postgres_single_connection, "apply_pending_migrations", lambda conn: []
    )

    def fake_connect(dsn):
        captured["dsn"] = dsn
        return DummyConn()

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", fake_connect)

    adapter = DummyAdapter("postgresql://example/dbname")
    adapter._connect()

    parsed = postgres_single_connection.psycopg2.extensions.parse_dsn(captured["dsn"])
    assert parsed["connect_timeout"] == "7"


def test_single_connection_adapter_preserves_existing_connect_timeout(monkeypatch):
    captured = {}

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return []

    class DummyConn:
        def __init__(self):
            self.encoding = "UTF8"
            self.closed = 0

        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

        def close(self):
            self.closed = 1

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "7")
    monkeypatch.setattr(
        postgres_single_connection, "apply_pending_migrations", lambda conn: []
    )

    def fake_connect(dsn):
        captured["dsn"] = dsn
        return DummyConn()

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", fake_connect)

    adapter = DummyAdapter("postgresql://example/dbname?connect_timeout=3")
    adapter._connect()

    parsed = postgres_single_connection.psycopg2.extensions.parse_dsn(captured["dsn"])
    assert parsed["connect_timeout"] == "3"


def test_keyword_ai_analysis_store_retries_connection_init_on_operational_error(
    monkeypatch,
):
    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn
            self._fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            self.conn.queries.append(query)
            if "INSERT INTO keyword_ai_analyses" in query and self.conn.fail_ddl:
                self.conn.fail_ddl = False
                raise postgres_single_connection.psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )
            if "RETURNING created_at" in query:
                self._fetchone = (datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),)

        def fetchone(self):
            return self._fetchone

        def fetchall(self):
            return []

    class DummyConn:
        def __init__(self, *, fail_ddl=False):
            self.fail_ddl = fail_ddl
            self.encoding = "UTF8"
            self.closed = 0
            self.queries = []
            self.commit_calls = 0
            self.rollback_calls = 0
            self.close_calls = 0

        def cursor(self):
            return DummyCursor(self)

        def commit(self):
            self.commit_calls += 1

        def rollback(self):
            self.rollback_calls += 1

        def close(self):
            self.close_calls += 1
            self.closed = 1

    first_conn = DummyConn(fail_ddl=True)
    second_conn = DummyConn()
    connections = [first_conn, second_conn]

    def fake_connect(dsn):
        parsed = postgres_single_connection.psycopg2.extensions.parse_dsn(dsn)
        assert parsed["host"] == "example"
        assert parsed["connect_timeout"] == "10"
        return connections.pop(0)

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(
        postgres_single_connection, "apply_pending_migrations", lambda conn: []
    )

    store = keyword_ai_analysis_postgres.PostgresKeywordAiAnalysisStore(
        "postgresql://example"
    )

    result = store.save_analysis(
        request_data={"trigger": "process"},
        analysis_input={
            "analyzed_keywords": 1,
            "total_candidates_before_limit": 1,
            "truncated": False,
            "keywords": [],
        },
        ai_analysis={
            "summary": "summary",
            "groups": [],
            "ungrouped_keyword_ids": [],
            "global_recommendations": [],
        },
        keyword_source="postgres",
        reporting_source="postgres",
        ai_model="test-model",
    )

    assert result["stored_items"] == 0
    assert result["created_at"] == "2026-03-20T12:00:00+00:00"
    assert connections == []
    assert first_conn.close_calls == 1
    assert second_conn.commit_calls == 2


def test_reporting_source_retries_read_after_operational_error(monkeypatch):
    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn
            self._rows = [
                (
                    "call-1",
                    "manager-1",
                    "Manager 1",
                    "sales",
                    "incoming",
                    0.1,
                    True,
                    "consultation",
                    "sale",
                    "summary",
                    12.5,
                    "20260320",
                    "111",
                    "222",
                    ["question"],
                    ["objection"],
                )
            ]
            self.fetchmany_calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if self.conn.fail_query:
                self.conn.fail_query = False
                raise postgres_single_connection.psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )

        def fetchmany(self, size):
            self.fetchmany_calls += 1
            if self.fetchmany_calls == 1:
                return self._rows
            return []

    class DummyConn:
        def __init__(self, *, fail_query=False):
            self.fail_query = fail_query
            self.encoding = "UTF8"
            self.closed = 0
            self.close_calls = 0

        def cursor(self):
            return DummyCursor(self)

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            self.close_calls += 1
            self.closed = 1

    first_conn = DummyConn(fail_query=True)
    second_conn = DummyConn()
    connections = [first_conn, second_conn]

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: connections.pop(0)
    )
    monkeypatch.setattr(
        postgres_single_connection, "apply_pending_migrations", lambda conn: []
    )

    source = reporting_postgres.PostgresReportingSource("postgresql://example")

    rows = list(
        source.iter_call_records(
            ReportFilters(
                date_from=None,
                date_to=None,
                manager_id=None,
                role=None,
                direction=None,
                intent=None,
                outcome=None,
                spam_only=False,
                effective_only=False,
            )
        )
    )

    assert len(rows) == 1
    assert rows[0].call_id == "call-1"
    # At least the first connection was used; second may or may not be consumed
    # depending on retry path within the generator.
    assert len(connections) <= 1


def test_keywords_source_retries_read_after_operational_error(monkeypatch):
    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if self.conn.fail_query:
                self.conn.fail_query = False
                raise postgres_single_connection.psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )

        def fetchall(self):
            return [
                ("delivery", "Delivery", "logistics", ["summary"], True, ["delivery"])
            ]

    class DummyConn:
        def __init__(self, *, fail_query=False):
            self.fail_query = fail_query
            self.encoding = "UTF8"
            self.closed = 0
            self.close_calls = 0
            self.commit_calls = 0

        def cursor(self):
            return DummyCursor(self)

        def commit(self):
            self.commit_calls += 1

        def close(self):
            self.close_calls += 1
            self.closed = 1

    first_conn = DummyConn(fail_query=True)
    second_conn = DummyConn()
    connections = [first_conn, second_conn]

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: connections.pop(0)
    )
    monkeypatch.setattr(
        postgres_single_connection, "apply_pending_migrations", lambda conn: []
    )

    source = keywords_postgres.PostgresKeywordSource("postgresql://example")

    rows = list(source.list_keywords())

    assert len(rows) == 1
    assert rows[0].keyword_id == "delivery"
    # Read operations don't call commit; verify retry succeeded with second connection
    assert len(connections) <= 1


def test_ollama_generate_sends_runtime_limits(monkeypatch):
    captured = {}

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"response": '{"ok":true}'}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr(llm_ollama.requests, "post", fake_post)

    config = SimpleNamespace(
        ollama_model="qwen3.5:27b",
        ollama_url="http://ai1.office.aviv.com.ua:11434",
        ollama_context_window=16384,
        ollama_keep_alive="10m",
        ollama_think=False,
        ollama_timeout=123,
        ollama_retries=1,
    )

    result = llm_ollama._ollama_generate(
        "hello", cast(AppConfig, config), temperature=0.1, force_json=True
    )

    assert result == '{"ok":true}'
    assert captured["url"] == "http://ai1.office.aviv.com.ua:11434/api/generate"
    assert captured["timeout"] == 123
    assert captured["json"]["keep_alive"] == "10m"
    assert captured["json"]["think"] is False
    assert captured["json"]["format"] == "json"
    assert captured["json"]["options"]["temperature"] == 0.1
    assert captured["json"]["options"]["num_ctx"] == 16384


def test_qdrant_storage_upsert_is_deterministic(monkeypatch):
    class DummyClient:
        def __init__(self, **kwargs):
            self.points = {}

        def upsert(self, collection_name, points):
            for p in points:
                self.points[p.id] = p

    monkeypatch.setattr("adapters.storage_qdrant.QdrantClient", DummyClient)
    storage = storage_qdrant.QdrantStorage()

    call_id = "call-determinism-test"
    embedding = [0.1] * 1024
    payload = {"meta": "data"}

    storage.upsert(call_id, embedding, payload)
    first_id = next(iter(cast(DummyClient, storage.client).points.keys()))  # type: ignore[union-attr]

    storage.upsert(call_id, embedding, payload)
    (
        list(cast(DummyClient, storage.client).points.keys())[-1]
        if len(cast(DummyClient, storage.client).points) > 1
        else first_id
    )

    # Re-instantiate to simulate restart/new process
    storage2 = storage_qdrant.QdrantStorage()
    storage2.client = storage.client  # share the mock client for verification
    storage2.upsert(call_id, embedding, payload)
    third_id = list(cast(DummyClient, storage.client).points.keys())[-1]  # type: ignore[union-attr]

    assert (
        first_id == third_id
    ), "Qdrant Point ID must be deterministic across calls and restarts"


def test_postgres_storage_ddl_contains_stt_promotion_columns():
    ddl = storage_postgres.DDL
    assert "stt_run_id UUID" in ddl
    assert "stt_config_hash TEXT" in ddl
    assert "source_text_sha256 TEXT" in ddl


# ---------------------------------------------------------------------------
# llm_ollama - _repair_truncated_json & _extract_json_object
# ---------------------------------------------------------------------------


def test_repair_truncated_json_closes_unterminated_string():
    """An unterminated string like {"key": "val should be repaired."""
    fragment = '{"enriched_candidates": [{"candidate_id": "1", "phrase": "deliv'
    result = llm_ollama._repair_truncated_json(fragment)
    assert result is not None
    assert result["enriched_candidates"][0]["phrase"] == "deliv"


def test_repair_truncated_json_closes_open_braces():
    fragment = '{"enriched_candidates": ['
    result = llm_ollama._repair_truncated_json(fragment)
    assert result is not None
    assert result["enriched_candidates"] == []


def test_repair_truncated_json_handles_nested_braces():
    fragment = '{"outer": {"inner": true'
    result = llm_ollama._repair_truncated_json(fragment)
    assert result is not None
    assert result["outer"]["inner"] is True


def test_repair_truncated_json_returns_none_for_garbage():
    fragment = 'not json at all ### $$$'
    result = llm_ollama._repair_truncated_json(fragment)
    assert result is None


def test_repair_truncated_json_handles_escaped_backslash_before_eof():
    fragment = '{"key": "value with backslash\\'
    result = llm_ollama._repair_truncated_json(fragment)
    assert result is not None
    assert "backslash" in result["key"]


def test_extract_json_object_falls_back_to_repair_on_truncated():
    """When the state machine finds no matching }, repair should kick in."""
    raw = 'Here is some text\n{"enriched_candidates": [{"candidate_id": "a1"}'
    result = llm_ollama._extract_json_object(raw)
    assert result["enriched_candidates"][0]["candidate_id"] == "a1"


def test_extract_json_object_raises_on_no_brace():
    raw = 'just plain text with no json here'
    with pytest.raises(ValueError, match="No JSON object found"):
        llm_ollama._extract_json_object(raw)


# ---------------------------------------------------------------------------
# reporting_postgres - _normalize_phone_sql
# ---------------------------------------------------------------------------


def test_normalize_phone_sql_escapes_percent_in_like_patterns():
    """LIKE patterns must use %% for literal % in psycopg2 param binding."""
    result = reporting_postgres.PostgresReportingSource._normalize_phone_sql(
        "src_number"
    )
    assert "LIKE '00%%'" in result
    assert "LIKE '0%%'" in result


def test_normalize_phone_sql_for_src_and_dst():
    sql_src = reporting_postgres.PostgresReportingSource._normalize_phone_sql(
        "src_number"
    )
    assert "src_number" in sql_src
    assert "regexp_replace" in sql_src

    sql_dst = reporting_postgres.PostgresReportingSource._normalize_phone_sql(
        "dst_number"
    )
    assert "dst_number" in sql_dst


def test_normalize_phone_sql_rejects_unknown_column():
    with pytest.raises(ValueError, match="Unexpected phone column"):
        reporting_postgres.PostgresReportingSource._normalize_phone_sql("phone_number")


# ---------------------------------------------------------------------------
# reporting_postgres - _match_count_sql
# ---------------------------------------------------------------------------


def test_match_count_sql_for_summary_field():
    sql = reporting_postgres.PostgresReportingSource._match_count_sql(
        "summary", "refund"
    )
    assert "LOWER(summary) LIKE %s" in sql
    assert "CASE WHEN" in sql


def test_match_count_sql_for_jsonb_array_fields():
    sql_kq = reporting_postgres.PostgresReportingSource._match_count_sql(
        "key_questions", "delivery"
    )
    assert "jsonb_array_elements_text(key_questions)" in sql_kq

    sql_obj = reporting_postgres.PostgresReportingSource._match_count_sql(
        "objections", "expensive"
    )
    assert "jsonb_array_elements_text(objections)" in sql_obj


def test_match_count_sql_for_unknown_field_returns_zero():
    sql = reporting_postgres.PostgresReportingSource._match_count_sql(
        "unknown_field", "term"
    )
    assert sql == "0"


def test_build_keywords_report_data_empty_keywords_returns_empty(monkeypatch):
    """When there are no keywords, build_keywords_report_data returns []."""
    source = reporting_postgres.PostgresReportingSource.__new__(
        reporting_postgres.PostgresReportingSource
    )
    result = source.build_keywords_report_data(
        keywords=[],
        filters=ReportFilters(),
        spam_threshold=0.7,
    )
    assert result == []


# ---------------------------------------------------------------------------
# reporting_postgres - build_keywords_report_data exists
# ---------------------------------------------------------------------------


def test_build_keywords_report_data_method_exists():
    assert hasattr(
        reporting_postgres.PostgresReportingSource, "build_keywords_report_data"
    )


def test_iter_call_records_method_exists():
    assert hasattr(reporting_postgres.PostgresReportingSource, "iter_call_records")


# ---------------------------------------------------------------------------
# keywords_postgres - batch_replace_call_keyword_matches
# ---------------------------------------------------------------------------


def test_batch_replace_call_keyword_matches_method_exists():
    assert hasattr(
        keywords_postgres.PostgresKeywordSource, "batch_replace_call_keyword_matches"
    )


def test_batch_replace_returns_early_for_empty_batches():
    source = keywords_postgres.PostgresKeywordSource.__new__(
        keywords_postgres.PostgresKeywordSource
    )
    source.batch_replace_call_keyword_matches([])


# ---------------------------------------------------------------------------
# keywords_postgres - ARRAY_AGG SQL pattern
# ---------------------------------------------------------------------------


def test_list_keywords_uses_array_agg():
    import inspect

    src = inspect.getsource(keywords_postgres.PostgresKeywordSource.list_keywords)
    assert "ARRAY_AGG" in src
    assert "LEFT JOIN keyword_aliases" in src
    assert "GROUP BY" in src


def test_get_keyword_uses_array_agg():
    import inspect

    src = inspect.getsource(keywords_postgres.PostgresKeywordSource.get_keyword)
    assert "ARRAY_AGG" in src
    assert "GROUP BY" in src


def test_build_keyword_calls_report_uses_joins():
    import inspect

    src = inspect.getsource(
        keywords_postgres.PostgresKeywordSource.build_keyword_calls_report
    )
    assert "JOIN analyses a" in src
    assert "call_keywords ck" in src


# ---------------------------------------------------------------------------
# postgres_single_connection - connect timeout helpers
# ---------------------------------------------------------------------------


def test_resolve_connect_timeout_default(monkeypatch):
    monkeypatch.delenv("POSTGRES_CONNECT_TIMEOUT", raising=False)
    assert postgres_single_connection._resolve_connect_timeout_seconds() == 10


def test_resolve_connect_timeout_custom_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "30")
    assert postgres_single_connection._resolve_connect_timeout_seconds() == 30


def test_resolve_connect_timeout_minimum_one(monkeypatch):
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "0")
    assert postgres_single_connection._resolve_connect_timeout_seconds() == 1


def test_resolve_connect_timeout_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "not_a_number")
    assert postgres_single_connection._resolve_connect_timeout_seconds() == 10


def test_dsn_with_connect_timeout_adds_param():
    dsn = "postgresql://user:pass@localhost/db"
    result = postgres_single_connection._dsn_with_connect_timeout(dsn)
    assert "connect_timeout" in result


def test_dsn_with_connect_timeout_preserves_existing():
    dsn = "postgresql://user:pass@localhost/db?connect_timeout=20"
    result = postgres_single_connection._dsn_with_connect_timeout(dsn)
    assert result.count("connect_timeout") == 1


# ---------------------------------------------------------------------------
# postgres_single_connection - _connect migration flow
# ---------------------------------------------------------------------------


def test_connect_sets_statement_timeout(monkeypatch):
    executed_statements: list[str] = []

    class MockCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params=None):
            executed_statements.append(sql)

    class MockConn:
        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

    mock_conn = MockConn()

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: mock_conn
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "_ensure_utf8_client_encoding",
        lambda conn: conn,
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "apply_pending_migrations",
        lambda conn: [],
    )

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    adapter = DummyAdapter(dsn="postgresql://user:pass@localhost/db")
    adapter._connect()

    timeout_stmts = [s for s in executed_statements if "statement_timeout" in s]
    assert len(timeout_stmts) == 1


def test_connect_calls_apply_pending_migrations(monkeypatch):
    migrations_called: list = []

    class MockCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params=None):
            pass

    class MockConn:
        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

    mock_conn = MockConn()

    def mock_apply_migrations(conn):
        migrations_called.append(conn)
        return []

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: mock_conn
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "_ensure_utf8_client_encoding",
        lambda conn: conn,
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "apply_pending_migrations",
        mock_apply_migrations,
    )

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    adapter = DummyAdapter(dsn="postgresql://user:pass@localhost/db")
    adapter._connect()

    assert len(migrations_called) == 1


def test_initialize_connection_called_after_migrations(monkeypatch):
    call_order: list[str] = []

    class MockCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params=None):
            pass

    class MockConn:
        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

    mock_conn = MockConn()

    def _fake_apply_migrations(conn):
        call_order.append("migrations")
        return []

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: mock_conn
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "_ensure_utf8_client_encoding",
        lambda conn: conn,
    )
    monkeypatch.setattr(
        postgres_single_connection,
        "apply_pending_migrations",
        _fake_apply_migrations,
    )

    class TestAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        def _initialize_connection(self, conn):
            call_order.append("init_conn")

    adapter = TestAdapter(dsn="postgresql://user:pass@localhost/db")
    adapter._connect()

    assert "migrations" in call_order
    assert "init_conn" in call_order
    assert call_order.index("migrations") < call_order.index("init_conn")


# ---------------------------------------------------------------------------
# Adapter subclasses no longer run DDL in _initialize_connection
# ---------------------------------------------------------------------------


def test_postgres_keyword_source_no_ddl_in_init():
    import inspect

    src = inspect.getsource(keywords_postgres.PostgresKeywordSource)
    assert "cur.execute(DDL)" not in src


def test_ai_alias_suggestions_no_ddl_in_init():
    from adapters.ai_alias_suggestions_postgres import PostgresAiAliasSuggestionStore

    import inspect

    src = inspect.getsource(PostgresAiAliasSuggestionStore)
    assert "cur.execute(DDL)" not in src


# ---------------------------------------------------------------------------
# migrations - schema migrations DDL, ALL_VERSIONS, apply_pending_migrations
# ---------------------------------------------------------------------------


from adapters.migrations import (
    ALL_VERSIONS,
    apply_pending_migrations,
    ensure_schema,
    get_schema_version,
    _get_applied_versions,
    SCHEMA_MIGRATIONS_DDL,
)


class MockCursor:
    def __init__(self, conn):
        self.conn = conn
        self.results = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if "SELECT version FROM schema_migrations" in sql:
            self.results = [[v] for v in self.conn.applied_versions]
        else:
            self.results = []

    def fetchall(self):
        return self.results


class MockConn:
    def __init__(self, applied_versions=None):
        self.applied_versions = applied_versions or set()
        self.executed = []
        self.commits = 0

    def cursor(self):
        return MockCursor(self)

    def commit(self):
        self.commits += 1


def test_ddl_creates_schema_migrations_table():
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in SCHEMA_MIGRATIONS_DDL
    assert "version" in SCHEMA_MIGRATIONS_DDL
    assert "applied_at" in SCHEMA_MIGRATIONS_DDL


def test_all_versions_not_empty():
    assert len(ALL_VERSIONS) >= 1


def test_all_versions_contains_v001():
    assert "V001" in ALL_VERSIONS


def test_all_versions_contains_v002():
    assert "V002" in ALL_VERSIONS


def test_all_versions_are_ordered():
    for i in range(len(ALL_VERSIONS) - 1):
        assert ALL_VERSIONS[i] < ALL_VERSIONS[i + 1]


def test_get_applied_versions_returns_set():
    conn = MockConn(applied_versions={"V001", "V002"})
    cur = conn.cursor()
    result = _get_applied_versions(cur)
    assert result == {"V001", "V002"}


def test_get_applied_versions_empty_when_no_migrations():
    conn = MockConn(applied_versions=set())
    cur = conn.cursor()
    result = _get_applied_versions(cur)
    assert result == set()


def test_apply_pending_creates_schema_migrations_table():
    conn = MockConn()
    apply_pending_migrations(conn)
    ddl_executed = any(
        "CREATE TABLE IF NOT EXISTS schema_migrations" in sql
        for sql, _ in conn.executed
    )
    assert ddl_executed


def test_apply_pending_skips_already_applied():
    conn = MockConn(applied_versions=set(ALL_VERSIONS))
    result = apply_pending_migrations(conn)
    assert result == []


def test_apply_pending_returns_list():
    conn = MockConn(applied_versions=set())
    result = apply_pending_migrations(conn)
    assert isinstance(result, list)


def test_get_schema_version_returns_latest():
    conn = MockConn(applied_versions={"V001", "V002"})
    result = get_schema_version(conn)
    assert result == "V002"


def test_get_schema_version_none_when_empty():
    conn = MockConn(applied_versions=set())
    result = get_schema_version(conn)
    assert result is None


def test_ensure_schema_is_alias_for_apply_pending():
    conn = MockConn(applied_versions=set(ALL_VERSIONS))
    ensure_schema(conn)
    ddl_executed = any(
        "CREATE TABLE IF NOT EXISTS schema_migrations" in sql
        for sql, _ in conn.executed
    )
    assert ddl_executed


def test_migration_files_exist_on_disk():
    from pathlib import Path

    migrations_dir = Path(__file__).parents[2] / "src" / "adapters" / "migrations"
    for version in ALL_VERSIONS:
        matches = list(migrations_dir.glob(f"{version}__*.sql"))
        assert len(matches) >= 1, f"No migration file found for {version}"


def test_deep_insights_no_ddl_in_init():
    from adapters.deep_insights_postgres import PostgresDeepInsightsStore

    import inspect

    src = inspect.getsource(PostgresDeepInsightsStore)
    assert "cur.execute(DDL)" not in src


def test_keyword_ai_analysis_no_ddl_in_init():
    import inspect

    src = inspect.getsource(
        keyword_ai_analysis_postgres.PostgresKeywordAiAnalysisStore
    )
    assert "cur.execute(DDL)" not in src
