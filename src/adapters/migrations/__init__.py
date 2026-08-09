from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Directory containing migration SQL files
_MIGRATIONS_DIR = os.path.dirname(__file__)

# Schema version tracking table
SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# All migration versions in order (monotonically increasing)
ALL_VERSIONS = [
    "V001",  # Core schema (tables, indexes, backfill)
    "V002",  # STT runs schema
]


def _get_applied_versions(cur: Any) -> set[str]:
    """Return set of already-applied migration versions."""
    cur.execute("SELECT version FROM schema_migrations ORDER BY version")
    return {row[0] for row in cur.fetchall()}


def apply_pending_migrations(conn: Any) -> list[str]:
    """Apply any pending schema migrations on the given connection.

    Returns list of applied version strings (empty if schema is current).
    """
    # Ensure schema_migrations table exists (lightweight, no locks on other tables)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_MIGRATIONS_DDL)
    conn.commit()

    # Check which versions are already applied
    with conn.cursor() as cur:
        applied = _get_applied_versions(cur)

    pending = [v for v in ALL_VERSIONS if v not in applied]
    if not pending:
        logger.debug("Schema is up-to-date (applied: %s)", sorted(applied))
        return []

    applied_new = []
    for version in pending:
        sql_path = os.path.join(_MIGRATIONS_DIR, f"{version}__core_schema.sql")
        if not os.path.isfile(sql_path):
            # Try generic pattern - any file matching the version prefix
            matches = [f for f in os.listdir(_MIGRATIONS_DIR) if f.startswith(f"{version}__")]
            if matches:
                sql_path = os.path.join(_MIGRATIONS_DIR, matches[0])
            else:
                logger.error("Migration file not found for version %s", version)
                continue

        with open(sql_path, "r") as f:
            sql = f.read()

        logger.info("Applying migration %s (%d statements)", version, sql.count(";"))
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s) ON CONFLICT DO NOTHING",
                (version,),
            )
        conn.commit()
        applied_new.append(version)
        logger.info("Migration %s applied successfully", version)

    if applied_new:
        logger.info(
            "Applied %d migration(s): %s", len(applied_new), ", ".join(applied_new)
        )

    return applied_new


def get_schema_version(conn: Any) -> str | None:
    """Return the latest applied schema version, or None if no migrations applied."""
    try:
        with conn.cursor() as cur:
            applied = _get_applied_versions(cur)
        return max(applied) if applied else None
    except Exception:
        return None


def ensure_schema(conn: Any) -> None:
    """Ensure schema is up-to-date. Alias for apply_pending_migrations."""
    applied = apply_pending_migrations(conn)
    if applied:
        logger.info("Schema updated: %s", ", ".join(applied))
