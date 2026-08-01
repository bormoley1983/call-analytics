from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Generator

import httpx
import pytest
from testcontainers.postgres import PostgresContainer

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

for path in (str(ROOT), str(SRC)):
    if path not in sys.path:
        sys.path.insert(0, path)

from adapters.keyword_ai_analysis_postgres import PostgresKeywordAiAnalysisStore
from adapters.keywords_postgres import PostgresKeywordSource
from adapters.storage_postgres import PostgresStorage

# ---------------------------------------------------------------------------
# Pytest configuration — register markers + skip integration by default
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (require external services)",
    )
    config.addinivalue_line(
        "markers", "postgres: marks tests as requiring a PostgreSQL instance"
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --run-integration flag to opt into integration tests."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires Docker for testcontainers)",
    )
    parser.addoption(
        "--regenerate-golden",
        action="store_true",
        default=False,
        help="Regenerate golden baseline files from current API responses",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip integration tests unless --run-integration is passed."""
    run_integration = config.getoption("--run-integration")
    if not run_integration:
        skip_integration = pytest.mark.skip(
            reason="Skipping integration test: pass --run-integration to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


def _docker_available() -> bool:
    """Check whether the Docker daemon is reachable."""
    import subprocess

    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Auto-skip postgres-marked tests when Docker is unavailable."""
    if "postgres" in item.keywords:
        if not _docker_available():
            pytest.skip("Docker is not available; cannot start testcontainers")


# ---------------------------------------------------------------------------
# PostgreSQL testcontainer (session-scoped — shared across all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for the entire test session."""
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    yield container
    container.stop(force=True)


@pytest.fixture(scope="session")
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    """Build DSN string from the running container."""
    # get_connection_url() returns "postgresql+psycopg2://..." — strip both parts
    dsn = postgres_container.get_connection_url()
    dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
    return dsn


# ---------------------------------------------------------------------------
# FastAPI test client
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def fastapi_app(postgres_dsn: str):
    """Create a FastAPI app wired to the test Postgres container."""
    import os

    # Set POSTGRES_DSN so API routes use Postgres backends instead of falling
    # back to read-only YAML mode (which returns 405 on write endpoints).
    os.environ["POSTGRES_DSN"] = postgres_dsn

    # Ensure schema is created before the app starts serving requests.
    # The API routes create their own Postgres adapters (reporting, keywords, etc.)
    # that don't run DDL on connect — they rely on PostgresStorage.ensure_ready()
    # to have already created all tables.
    storage = PostgresStorage(dsn=postgres_dsn, max_connections=5)
    storage.ensure_ready()
    storage.close()

    from api.app import app

    return app


@pytest.fixture
def api_client(fastapi_app):
    """TestClient for async API testing."""
    from starlette.testclient import TestClient

    client = TestClient(fastapi_app, raise_server_exceptions=False)
    return client


# ---------------------------------------------------------------------------
# Storage adapter (PostgresStorage — pool-based)
# ---------------------------------------------------------------------------


@pytest.fixture
def storage_adapter(postgres_dsn: str) -> Generator[PostgresStorage, None, None]:
    """PostgresStorage connected to the test container."""
    storage = PostgresStorage(dsn=postgres_dsn, max_connections=5)
    storage.ensure_ready()
    yield storage
    storage.close()


# ---------------------------------------------------------------------------
# Keywords adapter (SingleConnectionPostgresAdapter subclass)
# ---------------------------------------------------------------------------


@pytest.fixture
def keyword_adapter(postgres_dsn: str) -> Generator[PostgresKeywordSource, None, None]:
    """PostgresKeywordSource connected to the test container."""
    adapter = PostgresKeywordSource(dsn=postgres_dsn)
    yield adapter
    adapter.close()


# ---------------------------------------------------------------------------
# AI Analyses adapter (SingleConnectionPostgresAdapter subclass)
# ---------------------------------------------------------------------------


@pytest.fixture
def analyses_adapter(
    postgres_dsn: str,
) -> Generator[PostgresKeywordAiAnalysisStore, None, None]:
    """PostgresKeywordAiAnalysisStore connected to the test container."""
    adapter = PostgresKeywordAiAnalysisStore(dsn=postgres_dsn)
    yield adapter
    adapter.close()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_test_call(storage_adapter: PostgresStorage):
    """Seed a single test call with transcript + analysis rows."""
    call_id = "test_call_001"

    # Store transcript
    transcript_data = {
        "text": "Это тестовый звонок для интеграционных тестов.",
        "segments": [{"start": 0.0, "end": 5.0, "text": "Тестовый звонок"}],
        "_pipeline_stage": "translated",
        "text_uk": "Це тестовий дзвінок для інтеграційних тестів.",
        "segments_uk": [{"start": 0.0, "end": 5.0, "text": "Тестовий дзвінок"}],
    }
    storage_adapter.upsert_transcript(call_id, transcript_data)

    # Store analysis
    analysis_data = {
        "manager_id": "manager_001",
        "manager_name": "Test Manager",
        "role": "sales",
        "direction": "inbound",
        "spam_probability": 0.1,
        "effective_call": True,
        "intent": "purchase",
        "outcome": "positive",
        "summary": "Тестовый звонок - покупка товара",
        "call_meta": {
            "date": "2024-10-15",
            "src_number": "+79001234567",
            "dst_number": "+78009876543",
            "audio_seconds": 120.5,
        },
        "key_questions": ["Есть ли товар в наличии?", "Какая гарантия?"],
        "objections": ["Дорого"],
    }
    storage_adapter.upsert_analysis(call_id, analysis_data)

    return call_id
