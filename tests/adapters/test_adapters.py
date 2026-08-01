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

        def close(self):
            self.close_calls += 1
            self.closed = 1

    first_conn = DummyConn(fail_query=True)
    second_conn = DummyConn()
    connections = [first_conn, second_conn]

    monkeypatch.setattr(
        postgres_single_connection.psycopg2, "connect", lambda dsn: connections.pop(0)
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
    first_id = list(cast(DummyClient, storage.client).points.keys())[0]  # type: ignore[union-attr]

    storage.upsert(call_id, embedding, payload)
    second_id = (
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
