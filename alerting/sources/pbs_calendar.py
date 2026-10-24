"""Minimal parser for Proxmox Backup Server calendar-event schedule strings.

PBS reuses systemd's calendar-event syntax, but the Monitor only needs to
project a "next run" timestamp for the common formats that appear in PBS
config fields (`gc-schedule`, verify-job `schedule`, prune-job `schedule`).
Full systemd-timer compatibility is out of scope — anything we can't parse
returns `None` so callers can degrade gracefully instead of crashing.

Supported forms (all in UTC — PBS stores schedules without timezone info):

- ``"hourly"``                 → next full hour
- ``"daily"``                  → next 00:00 UTC
- ``"weekly"``                 → next Monday 00:00 UTC
- ``"monthly"``                → first day of next month, 00:00 UTC
- ``"*-*-* HH:MM:SS"``         → next matching UTC time, today or tomorrow
- ``"*-*-* HH:MM"``            → same, seconds = 0
- ``"<Weekdays> *-*-* HH:MM[:SS]"`` → next matching weekday + time, e.g.
  ``"Sat *-*-* 04:00"`` or ``"Mon,Fri *-*-* 00:00"``
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

_WEEKDAY_MAP = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")
_WILDCARD_DATE = "*-*-*"


def pbs_calendar_to_next_utc(
    spec: str | None, *, now: datetime | None = None
) -> datetime | None:
    """Return the next UTC datetime matching ``spec``, or ``None``."""

    if not spec or not isinstance(spec, str):
        return None
    text = spec.strip()
    if not text:
        return None

    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=UTC)

    lowered = text.lower()
    if lowered == "hourly":
        base = now.replace(minute=0, second=0, microsecond=0)
        return base + timedelta(hours=1)
    if lowered == "daily":
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return base + timedelta(days=1)
    if lowered == "weekly":
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        days_ahead = (0 - base.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return base + timedelta(days=days_ahead)
    if lowered == "monthly":
        year = now.year + (1 if now.month == 12 else 0)
        month = 1 if now.month == 12 else now.month + 1
        return datetime(year, month, 1, 0, 0, 0, tzinfo=UTC)

    tokens = text.split()
    if not tokens:
        return None

    weekdays: set[int] | None = None
    date_part: str | None = None
    time_part: str | None = None

    if len(tokens) == 2 and tokens[0] == _WILDCARD_DATE:
        date_part, time_part = tokens
    elif len(tokens) == 3 and tokens[1] == _WILDCARD_DATE:
        weekdays = _parse_weekday_list(tokens[0])
        if weekdays is None:
            return None
        date_part, time_part = tokens[1], tokens[2]
    else:
        return None

    if date_part != _WILDCARD_DATE:
        return None

    hms = _parse_hms(time_part)
    if hms is None:
        return None
    hour, minute, second = hms

    candidate = now.replace(
        hour=hour, minute=minute, second=second, microsecond=0,
    )
    if candidate <= now:
        candidate += timedelta(days=1)

    if weekdays is not None:
        for _ in range(7):
            if candidate.weekday() in weekdays:
                return candidate
            candidate += timedelta(days=1)
        return None

    return candidate


def _parse_hms(token: str) -> tuple[int, int, int] | None:
    match = _TIME_RE.match(token)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3)) if match.group(3) else 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None
    return hour, minute, second


def _parse_weekday_list(token: str) -> set[int] | None:
    result: set[int] = set()
    for name in token.split(","):
        key = name.strip().lower()[:3]
        if key not in _WEEKDAY_MAP:
            return None
        result.add(_WEEKDAY_MAP[key])
    return result or None
