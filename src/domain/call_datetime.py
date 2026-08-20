"""Call datetime parsing shared by core and adapters."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_call_datetime(date_str: str, time_str: str | None = None) -> datetime | None:
    """Parse PBX date (YYYYMMDD) and optional time (HHMMSS) into a timezone-aware datetime.

    Uses UTC as the default timezone since PBX systems typically report in UTC.
    Returns None if date_str is empty or invalid.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        if time_str:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc
            )
        # PBX systems typically report in UTC; attach UTC timezone for TIMESTAMPTZ
        return dt
    except ValueError:
        return None
