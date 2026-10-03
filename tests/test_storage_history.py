"""Tests for storage-history normalization, tiered downsampling, and the webui endpoint."""
import json

from alerting.normalization import (
    _SECONDS_PER_DAY,
    _STORAGE_HISTORY_TIER_KEYS,
    append_storage_sample,
    default_datastore_state,
    default_storage_history,
    migrate_state,
    normalize_storage_history,
    promote_storage_history,
    prune_storage_history,
)
from tests.conftest import do_login


def _sample(ts, used_percent=50.0, used_bytes=500_000_000_000, available_bytes=500_000_000_000):
    return {
        "ts": ts,
        "used_bytes": used_bytes,
        "available_bytes": available_bytes,
        "used_percent": used_percent,
    }


# ── normalize_storage_history ────────────────────────────────────────────────

def test_normalize_returns_four_tier_shape_for_none():
    out = normalize_storage_history(None)
    assert set(out) == set(_STORAGE_HISTORY_TIER_KEYS)
    assert all(out[tier] == [] for tier in _STORAGE_HISTORY_TIER_KEYS)


def test_normalize_drops_entries_without_valid_ts():
    history = {"raw": [_sample(None), _sample("not-int"), _sample(1700000000)]}
    out = normalize_storage_history(history)
    assert len(out["raw"]) == 1
    assert out["raw"][0]["ts"] == 1700000000


def test_normalize_sorts_each_tier_ascending():
    history = {
        "raw": [_sample(3000), _sample(1000), _sample(2000)],
        "hourly": [_sample(7200), _sample(3600)],
    }
    out = normalize_storage_history(history)
    assert [p["ts"] for p in out["raw"]] == [1000, 2000, 3000]
    assert [p["ts"] for p in out["hourly"]] == [3600, 7200]


def test_normalize_dedupes_by_ts_within_tier():
    history = {"raw": [_sample(1000, used_percent=10.0), _sample(1000, used_percent=20.0)]}
    out = normalize_storage_history(history)
    assert len(out["raw"]) == 1
    # Later entry wins on duplicate ts.
    assert out["raw"][0]["used_percent"] == 20.0


def test_normalize_coerces_numeric_fields_to_float():
    out = normalize_storage_history({"raw": [{"ts": 1000, "used_bytes": "123", "used_percent": "45.5"}]})
    point = out["raw"][0]
    assert point["used_bytes"] == 123.0
    assert point["used_percent"] == 45.5


# ── default_datastore_state ──────────────────────────────────────────────────

def test_default_datastore_state_has_empty_storage_history():
    ds = default_datastore_state("x")
    assert ds["storage_history"] == default_storage_history()
    assert set(ds["storage_history"]) == set(_STORAGE_HISTORY_TIER_KEYS)


# ── append_storage_sample ────────────────────────────────────────────────────

def test_append_appends_valid_sample():
    history = default_storage_history()
    appended = append_storage_sample(history, _sample(1000))
    assert appended is True
    assert len(history["raw"]) == 1
    assert history["raw"][0]["ts"] == 1000


def test_append_dedupes_when_prev_sample_within_min_interval():
    history = default_storage_history()
    append_storage_sample(history, _sample(1000))
    # Default min_sample_interval_seconds = 300.
    skipped = append_storage_sample(history, _sample(1100))
    assert skipped is False
    assert [p["ts"] for p in history["raw"]] == [1000]


def test_append_accepts_sample_past_min_interval():
    history = default_storage_history()
    append_storage_sample(history, _sample(1000))
    appended = append_storage_sample(history, _sample(1000 + 600))
    assert appended is True
    assert [p["ts"] for p in history["raw"]] == [1000, 1600]


def test_append_respects_config_min_interval_override():
    history = default_storage_history()
    config = {"storage_history": {"min_sample_interval_seconds": 60}}
    append_storage_sample(history, _sample(1000), config)
    appended = append_storage_sample(history, _sample(1070), config)
    assert appended is True
    assert [p["ts"] for p in history["raw"]] == [1000, 1070]


def test_append_rejects_invalid_sample():
    history = default_storage_history()
    assert append_storage_sample(history, None) is False
    assert append_storage_sample(history, {"ts": -1}) is False
    assert history["raw"] == []


# ── promote_storage_history ──────────────────────────────────────────────────

def test_promote_moves_old_raw_into_hourly_bucket_median():
    now = 100 * _SECONDS_PER_DAY  # arbitrary "now"
    old_bucket_start = now - 8 * _SECONDS_PER_DAY  # older than default raw retention 7d
    old_bucket_start -= old_bucket_start % 3600  # align to hourly bucket for determinism
    history = default_storage_history()
    history["raw"] = [
        _sample(old_bucket_start, used_percent=30.0),
        _sample(old_bucket_start + 60, used_percent=40.0),
        _sample(old_bucket_start + 120, used_percent=50.0),
    ]
    promote_storage_history(history, now_ts=now)
    assert history["raw"] == []
    assert len(history["hourly"]) == 1
    bucket = history["hourly"][0]
    assert bucket["ts"] == old_bucket_start
    assert bucket["used_percent"] == 40.0  # median of 30/40/50


def test_promote_keeps_recent_raw_samples():
    now = 100 * _SECONDS_PER_DAY
    history = default_storage_history()
    history["raw"] = [
        _sample(now - 3600),            # 1h old
        _sample(now - 3 * _SECONDS_PER_DAY),  # 3d old — still within 7d raw retention
    ]
    promote_storage_history(history, now_ts=now)
    assert len(history["raw"]) == 2
    assert history["hourly"] == []


def test_promote_cascades_hourly_to_sixhour_to_daily():
    now = 200 * _SECONDS_PER_DAY
    history = default_storage_history()
    # Older-than-hourly-retention (default 30d) → should land in sixhour.
    hourly_old_ts = now - 31 * _SECONDS_PER_DAY
    hourly_old_ts -= hourly_old_ts % 3600
    history["hourly"] = [_sample(hourly_old_ts, used_percent=42.0)]
    # Older-than-sixhour-retention (default 90d) → should cascade from sixhour to daily
    sixhour_old_ts = now - 95 * _SECONDS_PER_DAY
    sixhour_old_ts -= sixhour_old_ts % (6 * 3600)
    history["sixhour"] = [_sample(sixhour_old_ts, used_percent=66.0)]
    promote_storage_history(history, now_ts=now)
    assert history["hourly"] == []
    assert len(history["sixhour"]) == 1
    # sixhour bucket aligned to 6-hour grid of hourly_old_ts
    expected_sixhour_bucket = hourly_old_ts - (hourly_old_ts % (6 * 3600))
    assert history["sixhour"][0]["ts"] == expected_sixhour_bucket
    assert history["sixhour"][0]["used_percent"] == 42.0
    assert len(history["daily"]) == 1
    expected_daily_bucket = sixhour_old_ts - (sixhour_old_ts % _SECONDS_PER_DAY)
    assert history["daily"][0]["ts"] == expected_daily_bucket
    assert history["daily"][0]["used_percent"] == 66.0


def test_promote_preserves_existing_dst_bucket():
    now = 100 * _SECONDS_PER_DAY
    bucket_start = now - 10 * _SECONDS_PER_DAY
    bucket_start -= bucket_start % 3600
    history = default_storage_history()
    # Pre-existing hourly bucket should not be re-aggregated by late raw samples.
    history["hourly"] = [_sample(bucket_start, used_percent=99.0)]
    history["raw"] = [_sample(bucket_start + 60, used_percent=10.0)]
    promote_storage_history(history, now_ts=now)
    assert history["hourly"][0]["used_percent"] == 99.0
    assert history["raw"] == []  # aged sample dropped (bucket already present)


# ── prune_storage_history ────────────────────────────────────────────────────

def test_prune_does_nothing_when_daily_retention_none():
    history = default_storage_history()
    history["daily"] = [_sample(0, used_percent=10.0)]  # ancient
    prune_storage_history(history)  # defaults → daily_retention_days = None
    assert len(history["daily"]) == 1


def test_prune_drops_daily_points_past_retention():
    now = 500 * _SECONDS_PER_DAY
    history = default_storage_history()
    history["daily"] = [
        _sample(now - 400 * _SECONDS_PER_DAY, used_percent=10.0),
        _sample(now - 100 * _SECONDS_PER_DAY, used_percent=20.0),
    ]
    config = {"storage_history": {"daily_retention_days": 365}}
    prune_storage_history(history, config, now_ts=now)
    assert len(history["daily"]) == 1
    assert history["daily"][0]["used_percent"] == 20.0


# ── migrate_state ────────────────────────────────────────────────────────────

def test_migrate_state_adds_empty_storage_history_when_missing():
    raw = {"version": 2, "datastores": {"ds-1": {"name": "ds-1"}}}
    out = migrate_state(raw)
    assert out["datastores"]["ds-1"]["storage_history"] == default_storage_history()


def test_migrate_state_normalizes_existing_storage_history():
    raw = {
        "version": 2,
        "datastores": {
            "ds-1": {
                "name": "ds-1",
                "storage_history": {
                    "raw": [_sample(3000), _sample(1000), {"ts": "bad"}],
                    "hourly": "not-a-list",
                    "daily": [_sample(500)],
                },
            },
        },
    }
    out = migrate_state(raw)
    history = out["datastores"]["ds-1"]["storage_history"]
    assert [p["ts"] for p in history["raw"]] == [1000, 3000]
    assert history["hourly"] == []
    assert history["sixhour"] == []
    assert [p["ts"] for p in history["daily"]] == [500]


# ── /api/datastores/<ds>/storage-history endpoint ────────────────────────────

def _write_state(tmp_path, datastores):
    state = {
        "version": 2,
        "datastores": datastores,
        "last_alerts": {},
        "alert_suppress_until": {},
    }
    (tmp_path / "state.json").write_text(json.dumps(state))


def test_storage_history_endpoint_returns_empty_when_no_history(client_logged_in, tmp_path):
    client, _csrf = client_logged_in
    _write_state(tmp_path, {})
    resp = client.get("/api/datastores/ds-1/storage-history?range=30d")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["points"] == []
    assert body["source"] == "empty"
    assert body["range"] == "30d"


def test_storage_history_endpoint_rejects_invalid_range(client_logged_in, tmp_path):
    client, _csrf = client_logged_in
    _write_state(tmp_path, {})
    resp = client.get("/api/datastores/ds-1/storage-history?range=foo")
    assert resp.status_code == 400


def test_storage_history_endpoint_merges_tiers_for_range(client_logged_in, tmp_path):
    client, _csrf = client_logged_in
    import time
    now_ts = int(time.time())
    recent_ts = now_ts - 2 * _SECONDS_PER_DAY         # within 7d → in raw tier
    aged_hourly_ts = now_ts - 15 * _SECONDS_PER_DAY   # within 30d → in hourly tier
    aged_hourly_ts -= aged_hourly_ts % 3600
    far_daily_ts = now_ts - 200 * _SECONDS_PER_DAY    # outside 90d
    far_daily_ts -= far_daily_ts % _SECONDS_PER_DAY
    _write_state(tmp_path, {
        "ds-1": {
            "name": "ds-1",
            "storage_history": {
                "raw": [_sample(recent_ts, used_percent=55.0)],
                "hourly": [_sample(aged_hourly_ts, used_percent=66.0)],
                "sixhour": [],
                "daily": [_sample(far_daily_ts, used_percent=77.0)],
            },
        },
    })
    resp = client.get("/api/datastores/ds-1/storage-history?range=30d")
    assert resp.status_code == 200
    body = resp.get_json()
    tsset = {p["ts"] for p in body["points"]}
    assert recent_ts in tsset
    assert aged_hourly_ts in tsset
    assert far_daily_ts not in tsset  # 30d range → daily tier not included
    assert body["tiers"] == ["raw", "hourly"]
    assert body["sample_count"] == 2


def test_storage_history_endpoint_all_range_includes_every_tier(client_logged_in, tmp_path):
    client, _csrf = client_logged_in
    _write_state(tmp_path, {
        "ds-1": {
            "name": "ds-1",
            "storage_history": {
                "raw": [_sample(10_000)],
                "hourly": [_sample(5_000)],
                "sixhour": [_sample(3_000)],
                "daily": [_sample(1_000)],
            },
        },
    })
    resp = client.get("/api/datastores/ds-1/storage-history?range=all")
    assert resp.status_code == 200
    body = resp.get_json()
    assert [p["ts"] for p in body["points"]] == [1000, 3000, 5000, 10000]
    assert body["oldest_ts"] == 1000
    assert body["newest_ts"] == 10000
    assert body["tiers"] == ["raw", "hourly", "sixhour", "daily"]


def test_storage_history_endpoint_requires_auth(client_auth, tmp_path):
    _write_state(tmp_path, {})
    resp = client_auth.get("/api/datastores/ds-1/storage-history?range=7d")
    # Flow matches other require_auth endpoints: unauthenticated → 302 redirect to login.
    assert resp.status_code in (302, 401)


def test_storage_history_endpoint_without_auth_configured(client_no_auth, tmp_path):
    _write_state(tmp_path, {})
    resp = client_no_auth.get("/api/datastores/ds-1/storage-history?range=7d")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["points"] == []


# ── Suppress unused imports warning when running isolated ────────────────────
_ = do_login
