"""Tests for explicit server-side input validation on config and group-rule endpoints.

All write routes must return 400 with a descriptive ``error`` key for any
malformed or out-of-range input before touching any on-disk state.
"""
import pytest

from tests.conftest import TEST_PASSWORD, do_login, write_config, read_config
import webui.app as webapp


# ── Shared helpers ────────────────────────────────────────────────────────────

def _setup(client, tmp_path, monkeypatch):
    monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
    return do_login(client, TEST_PASSWORD)


def _post_config(client, csrf, payload):
    return client.post(
        "/api/alerting/config",
        json=payload,
        headers={"X-CSRF-Token": csrf},
    )


def _post_rule(client, csrf, payload, tmp_path, monkeypatch):
    monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
    import alerting.monitor as alert_monitor
    monkeypatch.setattr(alert_monitor, "GROUP_RULES_PATH", tmp_path / "group_rules.json")
    return client.post(
        "/api/alerting/group-rule",
        json=payload,
        headers={"X-CSRF-Token": csrf},
    )


# ── Config: string length limits ─────────────────────────────────────────────

class TestConfigStringLengths:
    def test_ntfy_url_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_url": "https://ntfy.sh/" + "a" * 2048})
        assert rv.status_code == 400
        assert "ntfy_url" in rv.get_json().get("error", "")

    def test_ntfy_topic_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_topic": "t" * 257})
        assert rv.status_code == 400
        assert "ntfy_topic" in rv.get_json().get("error", "")

    def test_ntfy_token_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_token": "x" * 513})
        assert rv.status_code == 400
        assert "ntfy_token" in rv.get_json().get("error", "")

    def test_ntfy_url_at_limit_is_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_url": "https://ntfy.sh/" + "a" * (2048 - len("https://ntfy.sh/"))})
        assert rv.status_code == 200


# ── Config: numeric range validation ─────────────────────────────────────────

class TestConfigNumericRanges:
    def test_negative_cooldown_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"alert_cooldown_minutes": -1})
        assert rv.status_code == 400
        assert "alert_cooldown_minutes" in rv.get_json().get("error", "")

    def test_zero_cooldown_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"alert_cooldown_minutes": 0})
        assert rv.status_code == 200

    def test_daemon_interval_below_60_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"daemon_interval_seconds": 59})
        assert rv.status_code == 400
        assert "daemon_interval_seconds" in rv.get_json().get("error", "")

    def test_daemon_interval_exactly_60_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"daemon_interval_seconds": 60})
        assert rv.status_code == 200

    def test_threshold_storage_warn_above_100_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"storage_warn_percent": 101}})
        assert rv.status_code == 400
        assert "storage_warn_percent" in rv.get_json().get("error", "")

    def test_threshold_zero_gc_hours_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"gc_max_age_hours": 0}})
        assert rv.status_code == 400
        assert "gc_max_age_hours" in rv.get_json().get("error", "")

    def test_notification_priority_zero_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"notification_priorities": {"critical": 0}})
        assert rv.status_code == 400
        assert "notification_priorities.critical" in rv.get_json().get("error", "")

    def test_notification_priority_6_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"notification_priorities": {"warning": 6}})
        assert rv.status_code == 400


# ── Config: time-format validation ───────────────────────────────────────────

class TestConfigTimeFormat:
    def test_quiet_hours_invalid_start_format_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"quiet_hours": {"start": "2200"}})
        assert rv.status_code == 400
        assert "quiet_hours.start" in rv.get_json().get("error", "")

    def test_quiet_hours_out_of_range_hour_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"quiet_hours": {"end": "25:00"}})
        assert rv.status_code == 400
        assert "quiet_hours.end" in rv.get_json().get("error", "")

    def test_quiet_hours_non_string_start_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"quiet_hours": {"start": 2200}})
        assert rv.status_code == 400

    def test_quiet_hours_valid_times_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"quiet_hours": {"start": "22:00", "end": "07:00"}})
        assert rv.status_code == 200

    def test_quiet_hours_min_priority_zero_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"quiet_hours": {"min_priority": 0}})
        assert rv.status_code == 400
        assert "min_priority" in rv.get_json().get("error", "")


# ── Group-rule: string length limits ─────────────────────────────────────────

class TestGroupRuleStringLengths:
    _VALID_BASE = {
        "datastore_id": "ds1",
        "backup_type": "vm",
        "backup_id": "100",
        "schedule_kind": "none",
    }

    def test_datastore_id_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        payload = {**self._VALID_BASE, "datastore_id": "d" * 129}
        rv = _post_rule(client_auth, csrf, payload, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "datastore_id" in rv.get_json().get("error", "")

    def test_backup_id_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        payload = {**self._VALID_BASE, "backup_id": "b" * 257}
        rv = _post_rule(client_auth, csrf, payload, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "backup_id" in rv.get_json().get("error", "")

    def test_display_name_too_long_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        payload = {**self._VALID_BASE, "display_name": "n" * 257}
        rv = _post_rule(client_auth, csrf, payload, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "display_name" in rv.get_json().get("error", "")


# ── Config: URL scheme validation ─────────────────────────────────────────────

class TestConfigUrlValidation:
    """ntfy_url and heartbeat_url must be http/https — other schemes rejected."""

    @pytest.mark.parametrize("bad_url", [
        "file:///etc/passwd",
        "gopher://evil.example",
        "ftp://evil.example",
        "javascript:alert(1)",
        "not-a-url-at-all",
    ])
    def test_ntfy_url_invalid_scheme_returns_400(self, monkeypatch, client_auth, tmp_path, bad_url):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_url": bad_url})
        assert rv.status_code == 400

    def test_ntfy_url_http_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_url": "http://ntfy.example.com"})
        assert rv.status_code == 200

    def test_ntfy_url_https_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"ntfy_url": "https://ntfy.sh"})
        assert rv.status_code == 200

    @pytest.mark.parametrize("bad_url", [
        "file:///etc/passwd",
        "gopher://evil.example",
        "ftp://evil.example",
    ])
    def test_heartbeat_url_invalid_scheme_returns_400(self, monkeypatch, client_auth, tmp_path, bad_url):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"heartbeat_url": bad_url})
        assert rv.status_code == 400

    def test_heartbeat_url_https_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"heartbeat_url": "https://healthchecks.io/ping/abc123"})
        assert rv.status_code == 200

    def test_heartbeat_url_empty_string_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"heartbeat_url": ""})
        assert rv.status_code == 200


# ── Config: corrupted config.json handling ────────────────────────────────────

class TestCorruptedConfigHandling:
    """Write endpoints must return 500 if config.json is not valid JSON."""

    def _write_corrupt_config(self, tmp_path):
        (tmp_path / "config.json").write_text("{invalid json")

    def test_save_config_corrupt_file_returns_500(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        self._write_corrupt_config(tmp_path)
        rv = _post_config(client_auth, csrf, {"ntfy_topic": "test"})
        assert rv.status_code == 500
        assert "corrupted" in rv.get_json().get("error", "").lower()

    def test_ignore_group_corrupt_file_returns_500(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        self._write_corrupt_config(tmp_path)
        rv = client_auth.post(
            "/api/alerting/ignore-group",
            json={"datastore_id": "ds1", "backup_type": "vm", "backup_id": "100"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 500
        assert "corrupted" in rv.get_json().get("error", "").lower()

    def test_unignore_group_corrupt_file_returns_500(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        self._write_corrupt_config(tmp_path)
        rv = client_auth.post(
            "/api/alerting/unignore-group",
            json={"datastore_id": "ds1", "backup_type": "vm", "backup_id": "100"},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 500
        assert "corrupted" in rv.get_json().get("error", "").lower()
