import pytest

from src.adapters import audio_ffmpeg, storage_json


def test_ffprobe_duration_seconds_exists():
    assert hasattr(audio_ffmpeg, "ffprobe_duration_seconds")

def test_json_storage_init(tmp_path):
    storage = storage_json.JsonStorage(tmp_path, tmp_path, tmp_path, tmp_path)
    assert storage.out == tmp_path