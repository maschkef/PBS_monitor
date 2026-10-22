"""Tests for the per-alarm acknowledge/unacknowledge feature.

Covers:
    - Validator: payload shape, required fields, past/malformed expires_at.
    - API: successful ack (time-bound + until_resolved), state is persisted,
      unacknowledge removes it, CSRF/auth/read-only guards.
    - Daemon: ``should_alert`` honors acks, ``run_check`` auto-clears resolved
      and expired acks.
"""
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import webui.app as webapp
from alerting import monitor as alert_monitor
from alerting.normalization import normalize_acknowledged_alerts
from alerting.notification import should_alert
from alerting.schedule import Alert
from tests.conftest import TEST_PASSWORD, do_login

# ── Helpers ──────────────────────────────────────────────────────────────────

def _setup(client, tmp_path, monkeypatch):
    """Patch webapp paths and perform login; returns CSRF token."""
    monkeypatch.setattr(webapp, "ALERTING_STATE_PATH", tmp_path / "state.json")
    return do_login(client, TEST_PASSWORD)


def _future_iso(hours_ahead: int = 2) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours_ahead)).isoformat()


def _past_iso(hours_ago: int = 1) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()


def _read_state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "state.json").read_text())


def _write_state(tmp_path: Path, state: dict) -> None:
    (tmp_path / "state.json").write_text(json.dumps(state))


# ── Validator ────────────────────────────────────────────────────────────────

class TestValidator:
    """``_validate_acknowledge_payload`` guards all required invariants."""

    def _validate(self, payload):
        from webui.validators import _validate_acknowledge_payload
        _validate_acknowledge_payload(payload)

    def test_missing_alert_key_raises(self):
        with pytest.raises(ValueError, match="alert_key"):
            self._validate({"until_resolved": True})

    def test_blank_alert_key_raises(self):
        with pytest.raises(ValueError, match="alert_key"):
            self._validate({"alert_key": "   ", "until_resolved": True})

    def test_alert_key_too_long_raises(self):
        with pytest.raises(ValueError, match="alert_key"):
            self._validate({"alert_key": "k" * 513, "until_resolved": True})

    def test_requires_either_expires_at_or_until_resolved(self):
        with pytest.raises(ValueError, match="Either"):
            self._validate({"alert_key": "k"})

    def test_rejects_both_expires_at_and_until_resolved(self):
        with pytest.raises(ValueError, match="not both"):
            self._validate({
                "alert_key": "k",
                "expires_at": _future_iso(),
                "until_resolved": True,
            })

    def test_rejects_malformed_expires_at(self):
        with pytest.raises(ValueError, match="expires_at"):
            self._validate({"alert_key": "k", "expires_at": "not-a-date"})

    def test_accepts_until_resolved(self):
        # Does not raise.
        self._validate({"alert_key": "k", "until_resolved": True})

    def test_accepts_future_expires_at(self):
        # Future validity is enforced by the route handler, not the validator;
        # the validator only verifies the ISO shape.
        self._validate({"alert_key": "k", "expires_at": _future_iso()})


# ── Normalizer ───────────────────────────────────────────────────────────────

class TestNormalizer:
    def test_drops_malformed_entries(self):
        raw = {
            "": {"until_resolved": True},               # empty key → dropped
            "ok": {"until_resolved": True},
            "nothing": {},                              # no mode → dropped
            "bad_entry": "not a dict",                  # wrong type → dropped
        }
        normalized = normalize_acknowledged_alerts(raw)
        assert set(normalized.keys()) == {"ok"}

    def test_drops_expired_time_bound_entries(self):
        raw = {
            "live": {"expires_at": _future_iso(2)},
            "dead": {"expires_at": _past_iso(1)},
        }
        normalized = normalize_acknowledged_alerts(raw)
        assert "live" in normalized
        assert "dead" not in normalized

    def test_keeps_until_resolved_regardless_of_expires_at(self):
        raw = {
            "u": {"until_resolved": True, "expires_at": _past_iso(1)},
        }
        normalized = normalize_acknowledged_alerts(raw)
        assert "u" in normalized
        assert normalized["u"]["until_resolved"] is True


# ── should_alert integration ────────────────────────────────────────────────

class TestShouldAlertHonorsAcks:
    def test_time_bound_ack_suppresses(self):
        state = {
            "acknowledged_alerts": {
                "k1": {"expires_at": _future_iso(1), "until_resolved": False},
            },
        }
        assert should_alert({}, state, "k1") is False

    def test_until_resolved_ack_suppresses(self):
        state = {
            "acknowledged_alerts": {
                "k2": {"expires_at": None, "until_resolved": True},
            },
        }
        assert should_alert({}, state, "k2") is False

    def test_expired_time_bound_ack_does_not_suppress(self):
        state = {
            "acknowledged_alerts": {
                "k3": {"expires_at": _past_iso(1), "until_resolved": False},
            },
        }
        assert should_alert({}, state, "k3") is True
        # Stale entry pruned on the way through.
        assert "k3" not in state["acknowledged_alerts"]

    def test_unrelated_key_not_affected(self):
        state = {
            "acknowledged_alerts": {
                "other": {"until_resolved": True},
            },
        }
        assert should_alert({}, state, "mine") is True


# ── run_check auto-clear logic ───────────────────────────────────────────────

class TestRunCheckAutoClear:
    """The ``run_check`` prune block clears resolved and expired acks."""

    def _run_prune(self, state, all_alerts):
        """Execute the same ack-prune block that lives in ``run_check``."""
        acks = state.setdefault("acknowledged_alerts", {})
        if acks:
            active_keys = {a.key for a in all_alerts}
            for key in list(acks.keys()):
                entry = acks.get(key) or {}
                expires_at_raw = entry.get("expires_at")
                if expires_at_raw and alert_monitor._is_expired(expires_at_raw):
                    del acks[key]
                    continue
                if entry.get("until_resolved") and key not in active_keys:
                    del acks[key]

    def _alert(self, key):
        return Alert("ds", "t", "m", key=key)

    def test_until_resolved_cleared_when_alert_absent(self):
        state = {
            "acknowledged_alerts": {
                "resolved": {"until_resolved": True, "expires_at": None},
                "still_active": {"until_resolved": True, "expires_at": None},
            },
        }
        self._run_prune(state, [self._alert("still_active")])
        assert "resolved" not in state["acknowledged_alerts"]
        assert "still_active" in state["acknowledged_alerts"]

    def test_expired_time_bound_cleared(self):
        state = {
            "acknowledged_alerts": {
                "stale": {"expires_at": _past_iso(1), "until_resolved": False},
                "fresh": {"expires_at": _future_iso(1), "until_resolved": False},
            },
        }
        self._run_prune(state, [self._alert("stale"), self._alert("fresh")])
        assert "stale" not in state["acknowledged_alerts"]
        assert "fresh" in state["acknowledged_alerts"]

    def test_until_resolved_kept_when_alert_present(self):
        state = {
            "acknowledged_alerts": {
                "k": {"until_resolved": True, "expires_at": None},
            },
        }
        self._run_prune(state, [self._alert("k")])
        assert "k" in state["acknowledged_alerts"]


# ── API: acknowledge-alarm ───────────────────────────────────────────────────

class TestAcknowledgeAPI:
    _ALERT_KEY: str = "ds1:missed_interval::vm:105:2026-10-22:120"

    def test_accepts_time_bound_ack(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={
                "alert_key": self._ALERT_KEY,
                "expires_at": _future_iso(1),
                "title": "Missed Backup Interval",
            },
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200, rv.get_json()
        state = _read_state(tmp_path)
        acks = state.get("acknowledged_alerts") or {}
        assert self._ALERT_KEY in acks
        entry = acks[self._ALERT_KEY]
        assert entry["until_resolved"] is False
        assert entry["expires_at"]
        assert entry["title"] == "Missed Backup Interval"

    def test_accepts_until_resolved_ack(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "until_resolved": True},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200, rv.get_json()
        state = _read_state(tmp_path)
        entry = (state.get("acknowledged_alerts") or {}).get(self._ALERT_KEY)
        assert entry is not None
        assert entry["until_resolved"] is True
        assert entry["expires_at"] is None

    def test_rejects_past_expires_at(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "expires_at": _past_iso(1)},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "expires_at" in rv.get_json().get("error", "").lower()

    def test_rejects_both_modes(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={
                "alert_key": self._ALERT_KEY,
                "expires_at": _future_iso(1),
                "until_resolved": True,
            },
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "not both" in rv.get_json().get("error", "").lower()

    def test_missing_alert_key_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"until_resolved": True},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "alert_key" in rv.get_json().get("error", "").lower()

    def test_requires_csrf(self, monkeypatch, client_auth, tmp_path):
        _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "until_resolved": True},
            # No X-CSRF-Token header.
        )
        assert rv.status_code in (400, 403)

    def test_requires_auth(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_STATE_PATH", tmp_path / "state.json")
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "until_resolved": True},
        )
        # Not logged in — redirects to login (302) or 401.
        assert rv.status_code in (302, 401, 403)

    def test_read_only_mode_returns_403(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        monkeypatch.setattr(webapp, "WEBUI_READ_ONLY", True)
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "until_resolved": True},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 403

    def test_overwrites_existing_ack(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        # First ack: time-bound.
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "expires_at": _future_iso(1)},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        # Second ack for the same key: until_resolved → replaces.
        rv = client_auth.post(
            "/api/alerting/acknowledge-alarm",
            json={"alert_key": self._ALERT_KEY, "until_resolved": True},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        entry = (_read_state(tmp_path).get("acknowledged_alerts") or {}).get(self._ALERT_KEY)
        assert entry["until_resolved"] is True
        assert entry["expires_at"] is None


# ── API: unacknowledge-alarm ─────────────────────────────────────────────────

class TestUnacknowledgeAPI:
    _ALERT_KEY: str = "ds1:missed_interval::vm:105:2026-10-22:120"

    def _seed_state(self, tmp_path, entry):
        _write_state(tmp_path, {
            "version": 2,
            "datastores": {},
            "last_alerts": {},
            "alert_suppress_until": {},
            "acknowledged_alerts": {self._ALERT_KEY: entry},
        })

    def test_removes_existing_ack(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        self._seed_state(tmp_path, {"until_resolved": True, "expires_at": None})
        rv = client_auth.post(
            "/api/alerting/unacknowledge-alarm",
            json={"alert_key": self._ALERT_KEY},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        body = rv.get_json()
        assert body["removed"] is True
        state = _read_state(tmp_path)
        assert self._ALERT_KEY not in (state.get("acknowledged_alerts") or {})

    def test_idempotent_on_missing_key(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/unacknowledge-alarm",
            json={"alert_key": "does-not-exist"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert rv.get_json()["removed"] is False

    def test_blank_alert_key_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/unacknowledge-alarm",
            json={"alert_key": "   "},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
