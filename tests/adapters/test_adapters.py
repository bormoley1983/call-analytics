from datetime import datetime, timezone
import pytest
from types import SimpleNamespace

from adapters import (
    audio_ffmpeg,
    keyword_ai_analysis_postgres,
    keywords_postgres,
    llm_ollama,
    postgres_single_connection,
    reporting_postgres,
    storage_json,
    storage_postgres,
)


def test_ffprobe_duration_seconds_exists():
    assert hasattr(audio_ffmpeg, "ffprobe_duration_seconds")

def test_json_storage_init(tmp_path):
    storage = storage_json.JsonStorage(tmp_path, tmp_path, tmp_path, tmp_path)
    assert storage.out == tmp_path


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


def test_single_connection_adapter_adds_default_connect_timeout(monkeypatch):
    captured = {}

    class DummyConn:
        def __init__(self):
            self.encoding = "UTF8"
            self.closed = 0

        def close(self):
            self.closed = 1

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "7")

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

    class DummyConn:
        def __init__(self):
            self.encoding = "UTF8"
            self.closed = 0

        def close(self):
            self.closed = 1

    class DummyAdapter(postgres_single_connection.SingleConnectionPostgresAdapter):
        pass

    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "7")

    def fake_connect(dsn):
        captured["dsn"] = dsn
        return DummyConn()

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", fake_connect)

    adapter = DummyAdapter("postgresql://example/dbname?connect_timeout=3")
    adapter._connect()

    parsed = postgres_single_connection.psycopg2.extensions.parse_dsn(captured["dsn"])
    assert parsed["connect_timeout"] == "3"


def test_keyword_ai_analysis_store_retries_connection_init_on_operational_error(monkeypatch):
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
            if query == keyword_ai_analysis_postgres.DDL and self.conn.fail_ddl:
                self.conn.fail_ddl = False
                raise postgres_single_connection.psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )
            if "RETURNING created_at" in query:
                self._fetchone = (datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),)

        def fetchone(self):
            return self._fetchone

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

    store = keyword_ai_analysis_postgres.PostgresKeywordAiAnalysisStore("postgresql://example")

    result = store.save_analysis(
        request_data={"trigger": "process"},
        analysis_input={
            "analyzed_keywords": 1,
            "total_candidates_before_limit": 1,
            "truncated": False,
            "keywords": [],
        },
        ai_analysis={"summary": "summary", "groups": [], "ungrouped_keyword_ids": [], "global_recommendations": []},
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

        def close(self):
            self.close_calls += 1
            self.closed = 1

    first_conn = DummyConn(fail_query=True)
    second_conn = DummyConn()
    connections = [first_conn, second_conn]

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", lambda dsn: connections.pop(0))

    source = reporting_postgres.PostgresReportingSource("postgresql://example")

    rows = list(source.iter_call_records(SimpleNamespace(
        call_date_from=None,
        call_date_to=None,
        manager_id=None,
        role=None,
        direction=None,
        intent=None,
        outcome=None,
    )))

    assert len(rows) == 1
    assert rows[0].call_id == "call-1"
    assert first_conn.close_calls == 1
    assert connections == []


def test_keywords_source_retries_read_after_operational_error(monkeypatch):
    class DummyCursor:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if query == keywords_postgres.DDL:
                return None
            if self.conn.fail_query:
                self.conn.fail_query = False
                raise postgres_single_connection.psycopg2.OperationalError(
                    "SSL connection has been closed unexpectedly"
                )

        def fetchall(self):
            return [("delivery", "Delivery", "logistics", ["summary"], True, ["delivery"])]

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

    monkeypatch.setattr(postgres_single_connection.psycopg2, "connect", lambda dsn: connections.pop(0))

    source = keywords_postgres.PostgresKeywordSource("postgresql://example")

    rows = list(source.list_keywords())

    assert len(rows) == 1
    assert rows[0].keyword_id == "delivery"
    assert first_conn.close_calls == 1
    assert second_conn.commit_calls == 1
    assert connections == []


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

    result = llm_ollama._ollama_generate("hello", config, temperature=0.1, force_json=True)

    assert result == '{"ok":true}'
    assert captured["url"] == "http://ai1.office.aviv.com.ua:11434/api/generate"
    assert captured["timeout"] == 123
    assert captured["json"]["keep_alive"] == "10m"
    assert captured["json"]["think"] is False
    assert captured["json"]["format"] == "json"
    assert captured["json"]["options"]["temperature"] == 0.1
    assert captured["json"]["options"]["num_ctx"] == 16384
