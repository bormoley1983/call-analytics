from __future__ import annotations

import os
from typing import Any, Callable, TypeVar

import psycopg2

from adapters.storage_postgres import _ensure_utf8_client_encoding

RETRYABLE_CONNECTION_ERRORS = (psycopg2.InterfaceError, psycopg2.OperationalError)
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
T = TypeVar("T")


def _resolve_connect_timeout_seconds() -> int:
    raw_value = os.getenv("POSTGRES_CONNECT_TIMEOUT", str(DEFAULT_CONNECT_TIMEOUT_SECONDS)).strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return DEFAULT_CONNECT_TIMEOUT_SECONDS


def _dsn_with_connect_timeout(dsn: str) -> str:
    params = psycopg2.extensions.parse_dsn(dsn)
    if params.get("connect_timeout"):
        return dsn
    return psycopg2.extensions.make_dsn(
        dsn,
        connect_timeout=str(_resolve_connect_timeout_seconds()),
    )


class SingleConnectionPostgresAdapter:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _initialize_connection(self, conn: Any) -> None:
        return None

    def _close_conn(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        try:
            if not conn.closed:
                conn.close()
        except Exception:
            pass

    def _connect(self):
        conn = _ensure_utf8_client_encoding(psycopg2.connect(_dsn_with_connect_timeout(self.dsn)))
        try:
            self._initialize_connection(conn)
        except Exception:
            try:
                if not conn.closed:
                    conn.close()
            finally:
                raise
        self._conn = conn
        return conn

    def _getconn(self):
        if self._conn is not None and not self._conn.closed:
            return self._conn

        last_error = None
        for _ in range(2):
            try:
                return self._connect()
            except RETRYABLE_CONNECTION_ERRORS as exc:
                last_error = exc
                self._close_conn()
        if last_error is not None:
            raise last_error
        raise RuntimeError("Unable to establish Postgres connection")

    def _rollback_quietly(self, conn: Any | None) -> None:
        if conn is None or getattr(conn, "closed", True):
            return
        try:
            conn.rollback()
        except Exception:
            self._close_conn()

    def _run_read(self, fn: Callable[[Any], T]) -> T:
        last_error = None
        for attempt in range(2):
            try:
                return fn(self._getconn())
            except RETRYABLE_CONNECTION_ERRORS as exc:
                last_error = exc
                self._close_conn()
                if attempt == 1:
                    raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("Read operation failed without returning a result")

    def _run_write(self, fn: Callable[[Any], T]) -> T:
        conn = None
        try:
            conn = self._getconn()
            result = fn(conn)
            conn.commit()
            return result
        except Exception:
            self._rollback_quietly(conn)
            raise

    def _run_retryable_write(
        self,
        fn: Callable[[Any], T],
        *,
        verify_after_retry: Callable[[], T | None] | None = None,
    ) -> T:
        last_error = None
        for attempt in range(2):
            conn = None
            try:
                conn = self._getconn()
                result = fn(conn)
                conn.commit()
                return result
            except RETRYABLE_CONNECTION_ERRORS as exc:
                last_error = exc
                self._rollback_quietly(conn)
                self._close_conn()
                if verify_after_retry is not None:
                    try:
                        existing = verify_after_retry()
                    except RETRYABLE_CONNECTION_ERRORS:
                        existing = None
                    if existing is not None:
                        return existing
                if attempt == 1:
                    raise
            except Exception:
                self._rollback_quietly(conn)
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("Write operation failed without returning a result")

    def close(self) -> None:
        self._close_conn()
