import pytest
from types import SimpleNamespace

from src.adapters import audio_ffmpeg, storage_json, storage_postgres
from src.adapters import llm_ollama


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
