from types import SimpleNamespace

import stt_compare
import stt_replay


class _FakePgStorage:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.ready = False

    def ensure_ready(self):
        self.ready = True


class _FakeJsonStorage:
    def __init__(self, out, norm, trans, analysis):
        self.out = out
        self.norm = norm
        self.trans = trans
        self.analysis = analysis
        self.ready = False

    def ensure_ready(self):
        self.ready = True


class _FakePgRunStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.ready = False

    def ensure_ready(self):
        self.ready = True


class _FakeJsonRunStore:
    def __init__(self, out):
        self.out = out
        self.ready = False

    def ensure_ready(self):
        self.ready = True


def _cfg():
    return SimpleNamespace(out="out", norm="norm", trans="trans", analysis="analysis")


def test_replay_uses_json_backends_without_postgres_dsn(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(stt_replay, "PostgresStorage", _FakePgStorage)
    monkeypatch.setattr(stt_replay, "JsonStorage", _FakeJsonStorage)
    monkeypatch.setattr(stt_replay, "PostgresSttRunStore", _FakePgRunStore)
    monkeypatch.setattr(stt_replay, "JsonSttRunStore", _FakeJsonRunStore)

    storage = stt_replay._build_storage(_cfg())
    run_store = stt_replay._build_run_store(_cfg())

    assert isinstance(storage, _FakeJsonStorage)
    assert isinstance(run_store, _FakeJsonRunStore)
    assert storage.ready is True
    assert run_store.ready is True


def test_replay_uses_postgres_backends_with_postgres_dsn(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgres://u:p@localhost:5432/db")
    monkeypatch.setattr(stt_replay, "PostgresStorage", _FakePgStorage)
    monkeypatch.setattr(stt_replay, "JsonStorage", _FakeJsonStorage)
    monkeypatch.setattr(stt_replay, "PostgresSttRunStore", _FakePgRunStore)
    monkeypatch.setattr(stt_replay, "JsonSttRunStore", _FakeJsonRunStore)

    storage = stt_replay._build_storage(_cfg())
    run_store = stt_replay._build_run_store(_cfg())

    assert isinstance(storage, _FakePgStorage)
    assert isinstance(run_store, _FakePgRunStore)
    assert storage.ready is True
    assert run_store.ready is True


def test_compare_uses_json_run_store_without_postgres_dsn(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(stt_compare, "PostgresSttRunStore", _FakePgRunStore)
    monkeypatch.setattr(stt_compare, "JsonSttRunStore", _FakeJsonRunStore)

    run_store = stt_compare._build_run_store(_cfg())

    assert isinstance(run_store, _FakeJsonRunStore)
    assert run_store.ready is True


def test_compare_uses_postgres_run_store_with_postgres_dsn(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgres://u:p@localhost:5432/db")
    monkeypatch.setattr(stt_compare, "PostgresSttRunStore", _FakePgRunStore)
    monkeypatch.setattr(stt_compare, "JsonSttRunStore", _FakeJsonRunStore)

    run_store = stt_compare._build_run_store(_cfg())

    assert isinstance(run_store, _FakePgRunStore)
    assert run_store.ready is True
