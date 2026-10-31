"""One-shot RRD backfill for the storage-history of a PBS datastore.

PBS keeps per-datastore usage samples in a built-in RRD exposed under
``/api2/json/admin/datastore/{store}/rrd?timeframe=…&cf=AVERAGE``.  New
PBS sources therefore start out with an empty chart in the dashboard
even though the underlying history already exists on the server.

This module performs a *one-shot* import of that RRD into the existing
four-tier ``storage_history`` structure (raw / hourly / sixhour / daily):

- Points carry ``source="backfill_rrd"`` so the dashboard can render them
  distinctly from live polled samples.
- A ``boundary_ts`` guard prevents the backfill from ever writing into
  the time range already covered by polling.
- Polled samples always win collisions at the same (snapped) ``ts``.

The module is intentionally decoupled from :mod:`alerting.sources.pbs`
so unit tests can exercise it with the recorded JSON fixtures under
``tests/e2e/pbs/rrd/`` instead of a live adapter.
"""

from __future__ import annotations

import requests

from alerting.normalization import (
    _SECONDS_PER_DAY,
    _STORAGE_HISTORY_BUCKET_SECONDS,
    _median,
    _storage_history_config,
    insert_backfill_point,
)
from alerting.sources.base import SourceConnectionError

TIMEFRAMES_DEFAULT: tuple[str, ...] = ("day", "month", "year", "decade")
CF_DEFAULT = "AVERAGE"
_UNAVAILABLE_CODES = (401, 403, 404)


def fetch_rrd_points(
    adapter, store: str, timeframe: str, cf: str = CF_DEFAULT,
) -> list[dict]:
    """GET /admin/datastore/{store}/rrd and normalize into storage-history points.

    PBS returns ``{"time": <epoch>, "used": <bytes>, "total": <bytes>,
    "available": <bytes>}``.  Empty slots are represented by rows that
    contain only ``time`` — those are dropped silently.

    Returned points use the project's internal key names (``ts``,
    ``used_bytes``, ``available_bytes``, ``used_percent``).  Lets HTTP
    errors propagate so the caller can distinguish 403 (missing ACL,
    treat as unavailable) from transport failures.
    """
    data = adapter._api_get(
        f"/api2/json/admin/datastore/{store}/rrd",
        params={"timeframe": timeframe, "cf": cf},
    ) or []
    points: list[dict] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        ts = row.get("time")
        used = row.get("used")
        total = row.get("total")
        available = row.get("available")
        if ts is None or used is None or total is None:
            continue
        try:
            ts_int = int(ts)
            used_f = float(used)
            total_f = float(total)
        except (TypeError, ValueError):
            continue
        if available is None:
            available_f = total_f - used_f
        else:
            try:
                available_f = float(available)
            except (TypeError, ValueError):
                available_f = total_f - used_f
        used_percent = (used_f / total_f * 100.0) if total_f > 0 else None
        points.append({
            "ts": ts_int,
            "used_bytes": used_f,
            "available_bytes": available_f,
            "used_percent": used_percent,
        })
    return points


def _oldest_polled_ts(storage_history: dict, default_ts: int) -> int:
    """Return the oldest ``ts`` across all tiers whose source is ``polled``.

    Legacy points without an explicit ``source`` are treated as polled.
    ``default_ts`` is returned when there are no polled points yet.
    """
    oldest: int | None = None
    if not isinstance(storage_history, dict):
        return default_ts
    for tier_key, entries in storage_history.items():
        if tier_key == "backfill_rrd_status" or not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            source = entry.get("source") or "polled"
            if source != "polled":
                continue
            ts = entry.get("ts")
            if not isinstance(ts, int):
                continue
            if oldest is None or ts < oldest:
                oldest = ts
    return default_ts if oldest is None else oldest


def _target_tier(age_seconds: int, resolved_cfg: dict) -> tuple[str, bool]:
    """Pick the destination tier for a point of a given age.

    Returns ``(tier, keep)``.  ``keep`` is False when the point is older
    than the configured daily retention — in that case the caller drops
    the point entirely.
    """
    raw_cutoff = resolved_cfg["raw_retention_days"] * _SECONDS_PER_DAY
    hourly_cutoff = resolved_cfg["hourly_retention_days"] * _SECONDS_PER_DAY
    sixhour_cutoff = resolved_cfg["sixhour_retention_days"] * _SECONDS_PER_DAY
    daily_retention = resolved_cfg["daily_retention_days"]
    if age_seconds < raw_cutoff:
        return "raw", True
    if age_seconds < hourly_cutoff:
        return "hourly", True
    if age_seconds < sixhour_cutoff:
        return "sixhour", True
    if daily_retention is not None and age_seconds >= daily_retention * _SECONDS_PER_DAY:
        return "daily", False
    return "daily", True


def _snap_ts(ts: int, tier: str) -> int:
    bucket_seconds = _STORAGE_HISTORY_BUCKET_SECONDS.get(tier)
    if bucket_seconds is None:
        return ts
    return ts - (ts % bucket_seconds)


def backfill_pbs_storage(
    adapter,
    store: str,
    storage_history: dict,
    config: dict | None,
    *,
    now_ts: int,
) -> dict:
    """Walk every configured timeframe and merge the RRD into storage_history.

    Returns a status dict suitable for persistence under
    ``storage_history["backfill_rrd_status"]``:

    - ``status`` ∈ {"ok", "unavailable"} — "error" is surfaced via raised
      exceptions so the monitor loop can categorize separately.
    - ``timeframes_fetched`` — list of PBS timeframe names that returned
      200.
    - ``points_added`` — count of newly inserted history points (does not
      include merges into existing backfill buckets).
    - ``oldest_ts`` — oldest ``ts`` written during this run, or None.
    - ``reason`` (unavailable only) — short human-readable summary.
    """
    pbs_cfg = (config or {}).get("pbs_backfill") or {}
    timeframes = pbs_cfg.get("timeframes") or list(TIMEFRAMES_DEFAULT)
    cf = pbs_cfg.get("cf") or CF_DEFAULT
    resolved_cfg = _storage_history_config(config)
    boundary_ts = _oldest_polled_ts(storage_history, default_ts=now_ts)

    timeframes_fetched: list[str] = []
    # Finer-timeframe-wins: iterate the config as given (fine → coarse) and
    # use setdefault so the earlier (finer) point for a given raw ``ts``
    # stays.  Collisions by raw ``ts`` across timeframes are rare in
    # practice because the resolutions differ.
    points_by_ts: dict[int, dict] = {}
    for tf in timeframes:
        try:
            rows = fetch_rrd_points(adapter, store, tf, cf=cf)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in _UNAVAILABLE_CODES:
                return {
                    "status": "unavailable",
                    "reason": f"HTTP {code} on timeframe={tf}",
                    "timeframes_fetched": list(timeframes_fetched),
                    "points_added": 0,
                    "oldest_ts": None,
                }
            raise
        timeframes_fetched.append(tf)
        for row in rows:
            points_by_ts.setdefault(row["ts"], row)

    buckets: dict[tuple[str, int], list[dict]] = {}
    for row in points_by_ts.values():
        if row["ts"] >= boundary_ts:
            continue
        age = now_ts - row["ts"]
        if age < 0:
            continue
        tier, keep = _target_tier(age, resolved_cfg)
        if not keep:
            continue
        snapped = _snap_ts(row["ts"], tier)
        buckets.setdefault((tier, snapped), []).append(row)

    points_added = 0
    oldest_written: int | None = None
    for (tier, snapped_ts), bucket_rows in buckets.items():
        merged = {
            "ts": snapped_ts,
            "used_bytes": _median(r["used_bytes"] for r in bucket_rows),
            "available_bytes": _median(r["available_bytes"] for r in bucket_rows),
            "used_percent": _median(r["used_percent"] for r in bucket_rows),
        }
        if insert_backfill_point(storage_history, merged, tier, boundary_ts):
            points_added += 1
            if oldest_written is None or snapped_ts < oldest_written:
                oldest_written = snapped_ts

    return {
        "status": "ok",
        "timeframes_fetched": timeframes_fetched,
        "points_added": points_added,
        "oldest_ts": oldest_written,
    }


__all__ = [
    "CF_DEFAULT",
    "TIMEFRAMES_DEFAULT",
    "SourceConnectionError",
    "backfill_pbs_storage",
    "fetch_rrd_points",
]
