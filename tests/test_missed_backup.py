"""Tests for the missed-backup downgrade + suppression behavior."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alerting import schedule as sched_mod
from alerting.notification import should_alert
from alerting.schedule import (
    build_missed_interval_alert,
    build_missed_slot_alert,
    evaluate_missed_backup_alerts,
)

TZ = ZoneInfo("Europe/Berlin")


def _ts(local_dt):
    """Local Berlin datetime → unix timestamp."""
    return int(local_dt.astimezone(timezone.utc).timestamp())


def _slot(minute_of_day=120, weekday_name="Daily", time_str="02:00"):
    return {
        "slot_key": f"daily:{time_str}",
        "minute_of_day": minute_of_day,
        "time": time_str,
        "weekday_name": weekday_name,
        "timezone": "Europe/Berlin",
        "last_observed_at": "None",
        "status": "active",
    }


def _ds():
    return {"id": "ds-1", "name": "datastore"}


def _group_state(snapshots=None):
    return {
        "namespace": "root",
        "backup_type": "vm",
        "backup_id": "51-pub",
        "display_name": "51-pub",
        "group_rule_key": "grk-1",
        "observed_snapshots": snapshots or [],
    }


# ── Builder-level tests ───────────────────────────────────────────────────────

def test_missed_slot_alert_downgrades_when_offschedule_snapshot_exists():
    due_dt = datetime(2026, 7, 9, 2, 0, tzinfo=TZ)
    off_dt = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)
    next_expected = datetime(2026, 7, 10, 2, 0, tzinfo=TZ).isoformat()

    alert = build_missed_slot_alert(
        _ds(),
        _group_state(),
        _slot(),
        due_dt,
        same_day_occurrences=[{"local_dt": off_dt}],
        next_expected_at=next_expected,
        downgrade_when_offschedule=True,
    )

    assert alert.priority == 3
    assert alert.suppress_until == next_expected
    assert "04:35" in alert.message
    assert "Suppressing further alerts" in alert.message


def test_missed_slot_alert_stays_high_when_no_offschedule_snapshot():
    due_dt = datetime(2026, 7, 9, 2, 0, tzinfo=TZ)

    alert = build_missed_slot_alert(
        _ds(),
        _group_state(),
        _slot(),
        due_dt,
        same_day_occurrences=[],
        next_expected_at=None,
        downgrade_when_offschedule=True,
    )

    assert alert.priority == 4
    assert alert.suppress_until is None
    assert "Off-schedule" not in alert.message


def test_missed_slot_alert_stays_high_when_downgrade_disabled():
    due_dt = datetime(2026, 7, 9, 2, 0, tzinfo=TZ)
    off_dt = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)

    alert = build_missed_slot_alert(
        _ds(),
        _group_state(),
        _slot(),
        due_dt,
        same_day_occurrences=[{"local_dt": off_dt}],
        next_expected_at="2026-07-10T02:00:00+02:00",
        downgrade_when_offschedule=False,
    )

    assert alert.priority == 4
    assert alert.suppress_until is None
    assert "04:35" in alert.message  # off-schedule info still included


def test_missed_interval_alert_downgrades_on_late_coverage():
    now_local = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)
    last_local = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)
    late_dt = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)
    next_expected = datetime(2026, 7, 9, 10, 0, tzinfo=TZ).isoformat()
    schedule_model = {
        "kind": "interval",
        "interval_minutes": 360,
        "interval_human": "every 6h",
        "timezone": "Europe/Berlin",
    }

    alert = build_missed_interval_alert(
        _ds(),
        _group_state(),
        schedule_model,
        now_local,
        last_local,
        late_coverage_occurrences=[{"local_dt": late_dt}],
        next_expected_at=next_expected,
        downgrade_when_offschedule=True,
    )

    assert alert.priority == 3
    assert alert.suppress_until == next_expected
    assert "Late-coverage" in alert.message


# ── should_alert() suppression tests ──────────────────────────────────────────

def test_should_alert_respects_active_suppression():
    future = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
    state = {"alert_suppress_until": {"k1": future}, "last_alerts": {}}
    assert should_alert({}, state, "k1") is False


def test_should_alert_clears_expired_suppression():
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    state = {"alert_suppress_until": {"k1": past}, "last_alerts": {}}
    assert should_alert({}, state, "k1") is True
    assert "k1" not in state["alert_suppress_until"]


def test_should_alert_falls_back_to_cooldown_when_no_suppression():
    state = {"alert_suppress_until": {}, "last_alerts": {}}
    assert should_alert({"alert_cooldown_minutes": 60}, state, "k1") is True

    # Now record a recent send → cooldown should block
    state["last_alerts"]["k1"] = datetime.now(timezone.utc).isoformat()
    assert should_alert({"alert_cooldown_minutes": 60}, state, "k1") is False


# ── End-to-end evaluate_missed_backup_alerts (with time patching) ────────────

def test_evaluate_daily_slot_downgrades_when_offschedule_exists(monkeypatch):
    now_local = datetime(2026, 7, 9, 5, 0, tzinfo=TZ)
    off_local = datetime(2026, 7, 9, 4, 35, tzinfo=TZ)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_local.astimezone(tz) if tz else now_local

    monkeypatch.setattr(sched_mod, "datetime", _FrozenDatetime)

    schedule_model = {
        "kind": "daily",
        "timezone": "Europe/Berlin",
        "slots": [{
            "slot_key": "daily:02:00",
            "minute_of_day": 120,
            "time": "02:00",
            "status": "active",
            "last_observed_at": "None",
        }],
    }
    group_state = _group_state(snapshots=[{"backup_time": _ts(off_local)}])
    config = {"schedule_learning": {
        "time_tolerance_minutes": 30,
        "due_grace_minutes": 30,
        "downgrade_when_offschedule": True,
    }}

    alerts = evaluate_missed_backup_alerts(_ds(), group_state, schedule_model, config, TZ)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.priority == 3
    assert alert.suppress_until is not None
    # Next expected should be the next slot (tomorrow 02:00 Europe/Berlin)
    assert alert.suppress_until.startswith("2026-07-10T02:00")


def test_evaluate_daily_slot_stays_high_without_offschedule(monkeypatch):
    now_local = datetime(2026, 7, 9, 5, 0, tzinfo=TZ)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_local.astimezone(tz) if tz else now_local

    monkeypatch.setattr(sched_mod, "datetime", _FrozenDatetime)

    schedule_model = {
        "kind": "daily",
        "timezone": "Europe/Berlin",
        "slots": [{
            "slot_key": "daily:02:00",
            "minute_of_day": 120,
            "time": "02:00",
            "status": "active",
            "last_observed_at": "None",
        }],
    }
    group_state = _group_state(snapshots=[])
    config = {"schedule_learning": {
        "time_tolerance_minutes": 30,
        "due_grace_minutes": 30,
        "downgrade_when_offschedule": True,
    }}

    alerts = evaluate_missed_backup_alerts(_ds(), group_state, schedule_model, config, TZ)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.priority == 4
    assert alert.suppress_until is None
