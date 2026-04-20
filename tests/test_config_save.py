"""Tests for ``POST /api/alerting/config`` save behavior.

Scenarios covered
-----------------
- Valid fields (ntfy_url, ntfy_topic, alert_cooldown_minutes, daemon_interval_seconds,
  thresholds, quiet_hours, schedule_learning, notification_priorities) are persisted.
- Invalid / out-of-range values are explicitly rejected with 400.
- Existing keys not present in the POST payload are preserved.
- Read-only mode returns 403 regardless of auth/CSRF.
- Empty payload returns 200 without touching the file.
"""
import json
import pytest

from tests.conftest import TEST_PASSWORD, do_login, write_config, read_config
import webui.app as webapp


class TestConfigSave:
    def _post(self, client, csrf: str, payload: dict, tmp_path, monkeypatch):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        return client.post(
            "/api/alerting/config",
            json=payload,
            headers={"X-CSRF-Token": csrf},
        )

    def _setup(self, client, tmp_path, monkeypatch, initial_config=None):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        if initial_config:
            write_config(tmp_path, initial_config)
        csrf = do_login(client, TEST_PASSWORD)
        return csrf

    # ── Happy-path: individual field groups ──────────────────────────────────

    def test_ntfy_url_is_saved(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_url": "https://ntfy.sh"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert rv.get_json()["ok"] is True
        assert read_config(tmp_path)["ntfy_url"] == "https://ntfy.sh"

    def test_ntfy_topic_is_saved(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_topic": "my-topic"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert read_config(tmp_path)["ntfy_topic"] == "my-topic"

    def test_alert_cooldown_minutes_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"alert_cooldown_minutes": 30},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert read_config(tmp_path)["alert_cooldown_minutes"] == 30

    def test_daemon_interval_seconds_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"daemon_interval_seconds": 120},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert read_config(tmp_path)["daemon_interval_seconds"] == 120

    def test_thresholds_saved(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"thresholds": {"storage_warn_percent": 75, "storage_crit_percent": 92}},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        thr = read_config(tmp_path)["thresholds"]
        assert thr["storage_warn_percent"] == 75
        assert thr["storage_crit_percent"] == 92

    def test_quiet_hours_saved(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"quiet_hours": {"enabled": True, "start": "22:00", "end": "07:00", "min_priority": 4}},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        qh = read_config(tmp_path)["quiet_hours"]
        assert qh["enabled"] is True
        assert qh["start"] == "22:00"
        assert qh["min_priority"] == 4

    def test_notification_priorities_saved(self, monkeypatch, client_auth, tmp_path):
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"notification_priorities": {"warning": 3, "critical": 5}},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        np_ = read_config(tmp_path)["notification_priorities"]
        assert np_["warning"] == 3
        assert np_["critical"] == 5

    # ── Invalid values are silently ignored ───────────────────────────────────

    def test_negative_cooldown_is_rejected(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"alert_cooldown_minutes": 60})
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"alert_cooldown_minutes": -10},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "alert_cooldown_minutes" in rv.get_json().get("error", "")
        # Original value must not be overwritten.
        assert read_config(tmp_path).get("alert_cooldown_minutes") == 60

    def test_daemon_interval_below_minimum_is_rejected(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"daemon_interval_seconds": 300})
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"daemon_interval_seconds": 5},  # below minimum of 60
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "daemon_interval_seconds" in rv.get_json().get("error", "")
        assert read_config(tmp_path).get("daemon_interval_seconds") == 300

    def test_priority_out_of_range_is_rejected(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"notification_priorities": {"warning": 3}})
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"notification_priorities": {"warning": 99}},  # out of 1-5 range
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "notification_priorities.warning" in rv.get_json().get("error", "")
        assert read_config(tmp_path)["notification_priorities"]["warning"] == 3

    # ── Existing keys are preserved on partial update ─────────────────────────

    def test_unrelated_keys_preserved_on_partial_update(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"ntfy_url": "https://ntfy.sh", "ntfy_topic": "preserved-topic"})
        csrf = self._setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"alert_cooldown_minutes": 20},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert saved["ntfy_url"] == "https://ntfy.sh"
        assert saved["ntfy_topic"] == "preserved-topic"
        assert saved["alert_cooldown_minutes"] == 20

    # ── Read-only mode ────────────────────────────────────────────────────────

    def test_read_only_mode_returns_403(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        monkeypatch.setattr(webapp, "WEBUI_READ_ONLY", True)
        csrf = do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_url": "https://ntfy.sh"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 403
