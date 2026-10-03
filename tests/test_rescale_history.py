"""Tests for persisted rescale-history normalization, merging, and migration."""
from alerting.normalization import (
    _MAX_RESCALE_HISTORY_ENTRIES,
    default_datastore_state,
    merge_rescale_entries,
    migrate_state,
    normalize_rescale_entries,
)


def _entry(ts, from_gb, to_gb, *, entry_id=None, reason="autoscale_up", automatic=True):
    return {
        "id": entry_id,
        "timestamp": ts,
        "from_gb": from_gb,
        "to_gb": to_gb,
        "reason": reason,
        "automatic": automatic,
    }


# ── normalize_rescale_entries ────────────────────────────────────────────────

def test_normalize_drops_entries_without_timestamp():
    raw = [
        _entry(None, 100, 200, entry_id="a"),
        _entry("", 100, 200, entry_id="b"),
        _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="c"),
    ]
    out = normalize_rescale_entries(raw)
    assert len(out) == 1
    assert out[0]["id"] == "c"


def test_normalize_dedupes_by_id():
    raw = [
        _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="dup"),
        _entry("2026-01-02T00:00:00Z", 200, 300, entry_id="dup"),
    ]
    out = normalize_rescale_entries(raw)
    assert len(out) == 1
    # Later occurrence wins (dict overwrite).
    assert out[0]["timestamp"] == "2026-01-02T00:00:00Z"


def test_normalize_dedupes_by_composite_when_id_missing():
    raw = [
        _entry("2026-01-01T00:00:00Z", 100, 200),
        _entry("2026-01-01T00:00:00Z", 100, 200, reason="manual_resize"),
    ]
    out = normalize_rescale_entries(raw)
    assert len(out) == 1


def test_normalize_sorts_newest_first():
    raw = [
        _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="old"),
        _entry("2026-03-15T04:00:00Z", 200, 300, entry_id="newest"),
        _entry("2026-02-01T00:00:00Z", 150, 200, entry_id="mid"),
    ]
    out = normalize_rescale_entries(raw)
    assert [e["id"] for e in out] == ["newest", "mid", "old"]


def test_normalize_respects_limit():
    raw = [
        _entry(f"2026-01-{day:02d}T00:00:00Z", 100, 200, entry_id=f"e{day}")
        for day in range(1, 11)
    ]
    out = normalize_rescale_entries(raw, limit=3)
    assert len(out) == 3
    assert out[0]["id"] == "e10"
    assert out[-1]["id"] == "e8"


def test_normalize_coerces_from_to_gb_to_int():
    raw = [_entry("2026-01-01T00:00:00Z", "250", "500", entry_id="x")]
    out = normalize_rescale_entries(raw)
    assert out[0]["from_gb"] == 250
    assert out[0]["to_gb"] == 500


def test_normalize_skips_non_dict_entries():
    raw = [None, "garbage", 42, _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="ok")]
    out = normalize_rescale_entries(raw)
    assert len(out) == 1
    assert out[0]["id"] == "ok"


# ── merge_rescale_entries ────────────────────────────────────────────────────

def test_merge_adds_new_entries():
    existing = [_entry("2026-01-01T00:00:00Z", 100, 200, entry_id="a")]
    new = [_entry("2026-02-01T00:00:00Z", 200, 300, entry_id="b")]
    out = merge_rescale_entries(existing, new, limit=100)
    assert {e["id"] for e in out} == {"a", "b"}


def test_merge_dedupes_overlap_by_id():
    existing = [_entry("2026-01-01T00:00:00Z", 100, 200, entry_id="shared")]
    new = [
        _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="shared"),
        _entry("2026-02-01T00:00:00Z", 200, 300, entry_id="new"),
    ]
    out = merge_rescale_entries(existing, new, limit=100)
    assert len(out) == 2


def test_merge_respects_limit():
    existing = [
        _entry(f"2026-01-{day:02d}T00:00:00Z", 100, 200, entry_id=f"e{day}")
        for day in range(1, 6)
    ]
    new = [
        _entry(f"2026-02-{day:02d}T00:00:00Z", 100, 200, entry_id=f"n{day}")
        for day in range(1, 6)
    ]
    out = merge_rescale_entries(existing, new, limit=3)
    assert len(out) == 3
    # Newest first → three February entries win.
    assert all(e["id"].startswith("n") for e in out)


# ── default_datastore_state ──────────────────────────────────────────────────

def test_default_datastore_state_has_empty_rescale_history():
    ds = default_datastore_state("my-ds")
    assert ds["rescale_history"] == []


# ── migrate_state ────────────────────────────────────────────────────────────

def test_migrate_state_initializes_missing_rescale_history():
    raw = {
        "version": 2,
        "datastores": {
            "ds-1": {"name": "ds-1", "backup_groups": {}},
        },
    }
    migrated = migrate_state(raw)
    assert migrated["datastores"]["ds-1"]["rescale_history"] == []


def test_migrate_state_normalizes_existing_rescale_history():
    raw = {
        "version": 2,
        "datastores": {
            "ds-1": {
                "name": "ds-1",
                "rescale_history": [
                    _entry("2026-02-01T00:00:00Z", 200, 300, entry_id="b"),
                    _entry("2026-01-01T00:00:00Z", 100, 200, entry_id="a"),
                    {"garbage": "no timestamp"},
                ],
            },
        },
    }
    migrated = migrate_state(raw)
    history = migrated["datastores"]["ds-1"]["rescale_history"]
    assert [e["id"] for e in history] == ["b", "a"]


def test_migrate_state_caps_rescale_history_at_module_limit():
    oversized = [
        _entry(f"2026-01-{day:02d}T00:00:00Z", 100, 200, entry_id=f"e{day:04d}")
        for day in range(1, _MAX_RESCALE_HISTORY_ENTRIES + 50)
        if day <= 28  # cap months at 28 days to keep ISO timestamps valid
    ]
    # Pad to overflow limit by varying hours (unique IDs + timestamps).
    extra_needed = _MAX_RESCALE_HISTORY_ENTRIES + 50 - len(oversized)
    for h in range(extra_needed):
        oversized.append(
            _entry(f"2026-02-01T{h % 24:02d}:00:{h // 24:02d}Z", 100, 200, entry_id=f"x{h:04d}")
        )
    raw = {
        "version": 2,
        "datastores": {
            "ds-1": {"name": "ds-1", "rescale_history": oversized},
        },
    }
    migrated = migrate_state(raw)
    assert len(migrated["datastores"]["ds-1"]["rescale_history"]) == _MAX_RESCALE_HISTORY_ENTRIES
