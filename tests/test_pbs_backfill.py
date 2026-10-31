"""Unit tests for alerting.sources.pbs_rrd using recorded PBS fixtures."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import requests

from alerting.normalization import (
    default_storage_history,
    insert_backfill_point,
    normalize_storage_history,
)
from alerting.sources.pbs_rrd import (
    TIMEFRAMES_DEFAULT,
    backfill_pbs_storage,
    fetch_rrd_points,
)

FIXTURE_DIR = Path(__file__).parent / "e2e" / "pbs" / "rrd"
STORE = "ssd"
# The decade fixture's newest row is 2026-10-19; pin "now" one minute past the
# latest row we have so the backfill treats every point as historical.
_NEWEST_FIXTURE_TS = 1793036805  # from rrd_hour.json (last row)
NOW_TS = _NEWEST_FIXTURE_TS + 60


def _load(name: str) -> dict:
    with (FIXTURE_DIR / name).open() as fp:
        return json.load(fp)


class FakeAdapter:
    """Minimal adapter stub that returns fixture JSON per timeframe."""

    def __init__(
        self,
        responses: dict[str, dict | Exception] | None = None,
    ):
        # Default: full happy path across every captured timeframe.
        self._responses: dict[str, dict | Exception] = responses or {
            "hour": _load("rrd_hour.json"),
            "day": _load("rrd_day.json"),
            "week": _load("rrd_week.json"),
            "month": _load("rrd_month.json"),
            "year": _load("rrd_year.json"),
            "decade": _load("rrd_decade.json"),
        }
        self.calls: list[tuple[str, dict]] = []

    def _api_get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        tf = (params or {}).get("timeframe")
        if tf not in self._responses:
            raise AssertionError(f"Unexpected timeframe fetched: {tf!r}")
        payload = self._responses[tf]
        if isinstance(payload, Exception):
            raise payload
        return payload.get("data")


def _http_error(status: int) -> requests.HTTPError:
    """Build a requests.HTTPError carrying the given status code."""
    resp = requests.Response()
    resp.status_code = status
    err = requests.HTTPError(f"{status} test", response=resp)
    return err


# ── fetch_rrd_points ─────────────────────────────────────────────────────────

def test_fetch_rrd_points_parses_dense_fixture():
    adapter = FakeAdapter()
    points = fetch_rrd_points(adapter, STORE, "hour")
    assert points, "hour fixture should yield points"
    # Every point must carry the four normalized keys we need downstream.
    p = points[0]
    assert set(p) >= {"ts", "used_bytes", "available_bytes", "used_percent"}
    assert isinstance(p["ts"], int)
    assert isinstance(p["used_bytes"], float)
    assert p["used_percent"] is not None
    assert 0.0 < p["used_percent"] < 100.0


def test_fetch_rrd_points_drops_sparse_rows():
    """Decade fixture has 518/523 rows missing `used` — must be filtered."""
    adapter = FakeAdapter()
    points = fetch_rrd_points(adapter, STORE, "decade")
    # Only 5 rows carry data; parser drops rows with just a `time` key.
    assert 0 < len(points) < 10


def test_fetch_rrd_points_passes_cf_param():
    adapter = FakeAdapter()
    fetch_rrd_points(adapter, STORE, "hour", cf="MAX")
    path, params = adapter.calls[-1]
    assert path.endswith("/admin/datastore/ssd/rrd")
    assert params["cf"] == "MAX"
    assert params["timeframe"] == "hour"


def test_fetch_rrd_points_computes_used_percent_in_range():
    """PBS returns `total` which can exceed `used + available` (e.g. ZFS
    reservations), so we test the resulting percentage is sensible rather
    than reconstructed from used + available."""
    adapter = FakeAdapter()
    points = fetch_rrd_points(adapter, STORE, "day")
    for p in points:
        assert p["used_percent"] is not None
        assert 0.0 < p["used_percent"] < 100.0


# ── backfill_pbs_storage ─────────────────────────────────────────────────────

def _happy_config() -> dict:
    return {
        "pbs_backfill": {
            "enabled": True,
            "timeframes": list(TIMEFRAMES_DEFAULT),
            "cf": "AVERAGE",
        },
    }


def test_backfill_populates_tiers():
    adapter = FakeAdapter()
    history = default_storage_history()
    result = backfill_pbs_storage(
        adapter, STORE, history, _happy_config(), now_ts=NOW_TS,
    )
    assert result["status"] == "ok"
    assert result["points_added"] > 0
    # Age-based placement: day fixture spans ~24h → raw; week fixture
    # spans ~7d (new ones land in raw, older in hourly); month → hourly.
    # The captured datastore is only ~34 days old so nothing is old enough
    # for the sixhour (>30d) or daily (>90d) tier.
    assert history["raw"], "day timeframe should populate raw tier"
    assert history["hourly"], "month timeframe should populate hourly tier"


def test_backfill_marks_every_inserted_point_as_backfill_rrd():
    adapter = FakeAdapter()
    history = default_storage_history()
    backfill_pbs_storage(adapter, STORE, history, _happy_config(), now_ts=NOW_TS)
    for tier in ("raw", "hourly", "sixhour", "daily"):
        for p in history[tier]:
            assert p["source"] == "backfill_rrd", f"{tier} has non-backfill point"


def test_backfill_respects_boundary_ts():
    """Polled sample at NOW blocks backfill from writing points >= its ts."""
    adapter = FakeAdapter()
    history = default_storage_history()
    polled_ts = NOW_TS - 3600  # one hour before "now"
    history["raw"].append({
        "ts": polled_ts,
        "used_bytes": 1.0,
        "available_bytes": 1.0,
        "used_percent": 50.0,
        "source": "polled",
    })
    backfill_pbs_storage(adapter, STORE, history, _happy_config(), now_ts=NOW_TS)
    for tier in ("raw", "hourly", "sixhour", "daily"):
        for p in history[tier]:
            if p["source"] == "backfill_rrd":
                assert p["ts"] < polled_ts, f"backfill breached boundary in {tier}"


def test_backfill_is_idempotent():
    """Running backfill twice must leave the history unchanged."""
    adapter_a = FakeAdapter()
    history = default_storage_history()
    backfill_pbs_storage(adapter_a, STORE, history, _happy_config(), now_ts=NOW_TS)
    snapshot = copy.deepcopy(history)

    adapter_b = FakeAdapter()
    backfill_pbs_storage(adapter_b, STORE, history, _happy_config(), now_ts=NOW_TS)
    assert history == snapshot


def test_backfill_does_not_overwrite_polled_at_same_bucket():
    """A polled point in a bucket wins against the backfill median."""
    adapter = FakeAdapter()
    history = default_storage_history()
    # Place a polled point far in the past so it falls into the ``daily`` tier
    # after age-based routing (daily bucket = 86400s, so pre-snap to midnight).
    old_ts = NOW_TS - 365 * 86400 * 2  # ~2 years back → sixhour/daily tier
    snapped = old_ts - (old_ts % 86400)
    sentinel_used = 123456789.0
    history["daily"].append({
        "ts": snapped,
        "used_bytes": sentinel_used,
        "available_bytes": 1.0,
        "used_percent": 42.0,
        "source": "polled",
    })

    backfill_pbs_storage(adapter, STORE, history, _happy_config(), now_ts=NOW_TS)

    # The polled sentinel must still be present untouched.
    matching = [p for p in history["daily"] if p["ts"] == snapped]
    assert len(matching) == 1
    assert matching[0]["source"] == "polled"
    assert matching[0]["used_bytes"] == sentinel_used


def test_backfill_returns_unavailable_on_403():
    adapter = FakeAdapter(responses={"day": _http_error(403)})
    history = default_storage_history()
    cfg = {"pbs_backfill": {"timeframes": ["day"]}}
    result = backfill_pbs_storage(adapter, STORE, history, cfg, now_ts=NOW_TS)
    assert result["status"] == "unavailable"
    assert "403" in result["reason"]
    assert result["points_added"] == 0
    # And nothing was written.
    assert all(not history[t] for t in ("raw", "hourly", "sixhour", "daily"))


def test_backfill_propagates_network_errors():
    """Non-ACL errors must raise so the monitor hook records ``status=error``."""
    adapter = FakeAdapter(responses={"day": requests.ConnectionError("boom")})
    history = default_storage_history()
    cfg = {"pbs_backfill": {"timeframes": ["day"]}}
    with pytest.raises(requests.ConnectionError):
        backfill_pbs_storage(adapter, STORE, history, cfg, now_ts=NOW_TS)


def test_backfill_oldest_ts_reflects_earliest_insert():
    adapter = FakeAdapter()
    history = default_storage_history()
    result = backfill_pbs_storage(
        adapter, STORE, history, _happy_config(), now_ts=NOW_TS,
    )
    all_ts = [
        p["ts"]
        for tier in ("raw", "hourly", "sixhour", "daily")
        for p in history[tier]
    ]
    assert result["oldest_ts"] == min(all_ts)


# ── insert_backfill_point helper ─────────────────────────────────────────────

def test_insert_backfill_point_blocks_by_boundary():
    history = default_storage_history()
    inserted = insert_backfill_point(
        history,
        {"ts": 1000, "used_bytes": 1.0, "available_bytes": 1.0, "used_percent": 50.0},
        "raw",
        boundary_ts=1000,
    )
    assert inserted is False
    assert history["raw"] == []


def test_insert_backfill_point_polled_first_wins():
    history = default_storage_history()
    history["raw"].append({
        "ts": 500,
        "used_bytes": 9.0,
        "available_bytes": 1.0,
        "used_percent": 90.0,
        "source": "polled",
    })
    inserted = insert_backfill_point(
        history,
        {"ts": 500, "used_bytes": 1.0, "available_bytes": 9.0, "used_percent": 10.0},
        "raw",
        boundary_ts=10_000,
    )
    assert inserted is False
    assert history["raw"][0]["used_bytes"] == 9.0


def test_insert_backfill_point_overwrites_existing_backfill():
    history = default_storage_history()
    history["raw"].append({
        "ts": 500,
        "used_bytes": 1.0,
        "available_bytes": 1.0,
        "used_percent": 50.0,
        "source": "backfill_rrd",
    })
    inserted = insert_backfill_point(
        history,
        {"ts": 500, "used_bytes": 7.0, "available_bytes": 3.0, "used_percent": 70.0},
        "raw",
        boundary_ts=10_000,
    )
    assert inserted is True
    assert history["raw"][0]["used_bytes"] == 7.0
    assert history["raw"][0]["source"] == "backfill_rrd"


def test_normalize_preserves_backfill_status_dict():
    history = {
        "raw": [],
        "hourly": [],
        "sixhour": [],
        "daily": [],
        "backfill_rrd_status": {"status": "ok", "points_added": 42},
    }
    out = normalize_storage_history(history)
    assert out["backfill_rrd_status"] == {"status": "ok", "points_added": 42}
