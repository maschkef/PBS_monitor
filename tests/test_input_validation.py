"""Tests for explicit server-side input validation on config and group-rule endpoints.

All write routes must return 400 with a descriptive ``error`` key for any
malformed or out-of-range input before touching any on-disk state.
"""
from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pytest

import webui.app as webapp
from tests.conftest import TEST_PASSWORD, do_login

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


# ── Config: storage_history validation ───────────────────────────────────────

class TestConfigStorageHistory:
    """Validation of the storage_history retention/sampling block."""

    def test_non_object_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": "not-an-object"})
        assert rv.status_code == 400
        assert "storage_history" in rv.get_json().get("error", "")

    @pytest.mark.parametrize(
        "field",
        ["raw_retention_days", "hourly_retention_days", "sixhour_retention_days"],
    )
    def test_zero_retention_returns_400(self, monkeypatch, client_auth, tmp_path, field):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {field: 0}})
        assert rv.status_code == 400
        assert f"storage_history.{field}" in rv.get_json().get("error", "")

    @pytest.mark.parametrize(
        "field",
        ["raw_retention_days", "hourly_retention_days", "sixhour_retention_days"],
    )
    def test_negative_retention_returns_400(self, monkeypatch, client_auth, tmp_path, field):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {field: -3}})
        assert rv.status_code == 400

    @pytest.mark.parametrize(
        "field",
        ["raw_retention_days", "hourly_retention_days", "sixhour_retention_days"],
    )
    def test_non_integer_retention_returns_400(self, monkeypatch, client_auth, tmp_path, field):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {field: "many"}})
        assert rv.status_code == 400

    def test_daily_retention_null_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"daily_retention_days": None}})
        assert rv.status_code == 200

    def test_daily_retention_empty_string_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"daily_retention_days": ""}})
        assert rv.status_code == 200

    def test_daily_retention_zero_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"daily_retention_days": 0}})
        assert rv.status_code == 400
        assert "storage_history.daily_retention_days" in rv.get_json().get("error", "")

    def test_daily_retention_positive_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"daily_retention_days": 365}})
        assert rv.status_code == 200

    def test_min_sample_interval_below_60_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"min_sample_interval_seconds": 59}})
        assert rv.status_code == 400
        assert "storage_history.min_sample_interval_seconds" in rv.get_json().get("error", "")

    def test_min_sample_interval_60_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"min_sample_interval_seconds": 60}})
        assert rv.status_code == 200

    def test_full_valid_block_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {
            "raw_retention_days": 7,
            "hourly_retention_days": 30,
            "sixhour_retention_days": 90,
            "daily_retention_days": None,
            "min_sample_interval_seconds": 300,
            "rescale_history_max_entries": 1500,
        }})
        assert rv.status_code == 200

    def test_rescale_max_zero_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"rescale_history_max_entries": 0}})
        assert rv.status_code == 400
        assert "rescale_history_max_entries" in rv.get_json().get("error", "")

    def test_rescale_max_negative_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"rescale_history_max_entries": -5}})
        assert rv.status_code == 400

    def test_rescale_max_non_integer_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"rescale_history_max_entries": "lots"}})
        assert rv.status_code == 400

    def test_rescale_max_positive_persists(self, monkeypatch, client_auth, tmp_path):
        import json as _json
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"storage_history": {"rescale_history_max_entries": 2500}})
        assert rv.status_code == 200
        with open(tmp_path / "config.json") as f:
            saved = _json.load(f)
        assert saved["storage_history"]["rescale_history_max_entries"] == 2500


# ── Config: replication-stale threshold validation ──────────────────────────

class TestConfigReplicationStaleThreshold:
    """Validation of the new replication-stale multiplier/grace keys under thresholds."""

    def test_multiplier_at_1_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_multiplier": 1.0}})
        assert rv.status_code == 400
        assert "replication_stale_multiplier" in rv.get_json().get("error", "")

    def test_multiplier_below_1_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_multiplier": 0.5}})
        assert rv.status_code == 400

    def test_multiplier_non_numeric_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_multiplier": "soon"}})
        assert rv.status_code == 400

    def test_multiplier_above_1_persists(self, monkeypatch, client_auth, tmp_path):
        import json as _json
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_multiplier": 2.5}})
        assert rv.status_code == 200
        with open(tmp_path / "config.json") as f:
            saved = _json.load(f)
        assert saved["thresholds"]["replication_stale_multiplier"] == 2.5

    def test_grace_negative_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_grace_minutes": -1}})
        assert rv.status_code == 400
        assert "replication_stale_grace_minutes" in rv.get_json().get("error", "")

    def test_grace_zero_is_valid(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_grace_minutes": 0}})
        assert rv.status_code == 200

    def test_grace_positive_persists(self, monkeypatch, client_auth, tmp_path):
        import json as _json
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {"thresholds": {"replication_stale_grace_minutes": 60}})
        assert rv.status_code == 200
        with open(tmp_path / "config.json") as f:
            saved = _json.load(f)
        assert saved["thresholds"]["replication_stale_grace_minutes"] == 60


# ── Config: observed_snapshot_history_max validation ────────────────────────

class TestConfigObservedSnapshotHistoryMax:
    """Validation of the new observed_snapshot_history_max key under schedule_learning."""

    def test_zero_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {
            "schedule_learning": {"observed_snapshot_history_max": 0}
        })
        assert rv.status_code == 400
        assert "observed_snapshot_history_max" in rv.get_json().get("error", "")

    def test_negative_returns_400(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {
            "schedule_learning": {"observed_snapshot_history_max": -10}
        })
        assert rv.status_code == 400

    def test_positive_persists(self, monkeypatch, client_auth, tmp_path):
        import json as _json
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post_config(client_auth, csrf, {
            "schedule_learning": {"observed_snapshot_history_max": 2000}
        })
        assert rv.status_code == 200
        with open(tmp_path / "config.json") as f:
            saved = _json.load(f)
        assert saved["schedule_learning"]["observed_snapshot_history_max"] == 2000


# ── Group-rule: string length limits ─────────────────────────────────────────

class TestGroupRuleStringLengths:
    _VALID_BASE: ClassVar[dict[str, str]] = {
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


class TestIgnoreGroupExpiresAt:
    """Time-limited ignore via the ``expires_at`` field."""

    @staticmethod
    def _future_iso(hours_ahead: int = 24) -> str:
        return (datetime.now(UTC) + timedelta(hours=hours_ahead)).isoformat()

    @staticmethod
    def _past_iso(hours_ago: int = 1) -> str:
        return (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()

    def _payload(self, **overrides):
        base = {
            "datastore_id": "ds1",
            "namespace": "",
            "backup_type": "vm",
            "backup_id": "100",
            "display_name": "test-vm",
        }
        base.update(overrides)
        return base

    def test_accepts_future_expires_at(self, monkeypatch, client_auth, tmp_path):
        from tests.conftest import read_config
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        expires_at = self._future_iso(24)
        rv = client_auth.post(
            "/api/alerting/ignore-group",
            json=self._payload(expires_at=expires_at),
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200, rv.get_json()
        persisted = read_config(tmp_path)
        ignored = persisted.get("ignored_groups") or []
        assert len(ignored) == 1
        assert ignored[0].get("expires_at")  # non-null ISO string

    def test_rejects_past_expires_at(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/ignore-group",
            json=self._payload(expires_at=self._past_iso(1)),
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "expires_at" in rv.get_json().get("error", "").lower()

    def test_rejects_malformed_expires_at(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/ignore-group",
            json=self._payload(expires_at="not-a-date"),
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 400
        assert "expires_at" in rv.get_json().get("error", "").lower()

    def test_null_expires_at_is_permanent(self, monkeypatch, client_auth, tmp_path):
        from tests.conftest import read_config
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/ignore-group",
            json=self._payload(expires_at=None),
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200, rv.get_json()
        persisted = read_config(tmp_path)
        ignored = persisted.get("ignored_groups") or []
        assert len(ignored) == 1
        assert ignored[0].get("expires_at") is None


class TestPurgeExpiredIgnoredGroups:
    """``purge_expired_ignored_groups`` filters out past entries, keeps the rest."""

    def test_filters_past_entries_keeps_future_and_permanent(self):
        from alerting.normalization import (
            normalize_ignored_groups,
            purge_expired_ignored_groups,
        )
        now = datetime.now(UTC)
        raw = [
            {  # expired an hour ago
                "datastore_id": "ds1", "namespace": "", "backup_type": "vm", "backup_id": "100",
                "expires_at": (now - timedelta(hours=1)).isoformat(),
            },
            {  # still valid
                "datastore_id": "ds1", "namespace": "", "backup_type": "vm", "backup_id": "101",
                "expires_at": (now + timedelta(hours=1)).isoformat(),
            },
            {  # permanent
                "datastore_id": "ds1", "namespace": "", "backup_type": "ct", "backup_id": "200",
                "expires_at": None,
            },
        ]
        normalized = normalize_ignored_groups(raw)
        kept = purge_expired_ignored_groups(normalized)
        kept_ids = {entry["backup_id"] for entry in kept}
        assert kept_ids == {"101", "200"}
