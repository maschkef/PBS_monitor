"""Tests for the config-value resolver helpers in alerting.monitor."""

import json

import pytest

from alerting import monitor as alert_monitor
from alerting.monitor import (
    MAX_OBSERVED_SNAPSHOT_HISTORY,
    resolve_observed_snapshot_history_max,
    resolve_replication_stale_params,
    resolve_rescale_history_max,
)
from alerting.normalization import _MAX_RESCALE_HISTORY_ENTRIES


class TestResolveRescaleHistoryMax:
    def test_missing_config_returns_fallback(self):
        assert resolve_rescale_history_max(None) == _MAX_RESCALE_HISTORY_ENTRIES
        assert resolve_rescale_history_max({}) == _MAX_RESCALE_HISTORY_ENTRIES

    def test_missing_key_returns_fallback(self):
        assert resolve_rescale_history_max({"storage_history": {}}) == _MAX_RESCALE_HISTORY_ENTRIES

    def test_invalid_value_returns_fallback(self):
        assert resolve_rescale_history_max(
            {"storage_history": {"rescale_history_max_entries": 0}}
        ) == _MAX_RESCALE_HISTORY_ENTRIES
        assert resolve_rescale_history_max(
            {"storage_history": {"rescale_history_max_entries": "lots"}}
        ) == _MAX_RESCALE_HISTORY_ENTRIES

    def test_positive_value_wins(self):
        assert resolve_rescale_history_max(
            {"storage_history": {"rescale_history_max_entries": 2500}}
        ) == 2500


class TestResolveObservedSnapshotHistoryMax:
    def test_missing_config_returns_fallback(self):
        assert resolve_observed_snapshot_history_max(None) == MAX_OBSERVED_SNAPSHOT_HISTORY

    def test_invalid_value_returns_fallback(self):
        assert resolve_observed_snapshot_history_max(
            {"schedule_learning": {"observed_snapshot_history_max": -5}}
        ) == MAX_OBSERVED_SNAPSHOT_HISTORY

    def test_positive_value_wins(self):
        assert resolve_observed_snapshot_history_max(
            {"schedule_learning": {"observed_snapshot_history_max": 1500}}
        ) == 1500


class TestResolveReplicationStaleParams:
    def test_defaults_when_missing(self):
        assert resolve_replication_stale_params(None) == (2.0, 30)
        assert resolve_replication_stale_params({}) == (2.0, 30)
        assert resolve_replication_stale_params({"thresholds": {}}) == (2.0, 30)

    def test_multiplier_le_one_falls_back(self):
        mult, _ = resolve_replication_stale_params(
            {"thresholds": {"replication_stale_multiplier": 1.0}}
        )
        assert mult == 2.0
        mult, _ = resolve_replication_stale_params(
            {"thresholds": {"replication_stale_multiplier": 0.5}}
        )
        assert mult == 2.0

    def test_non_numeric_multiplier_falls_back(self):
        mult, _ = resolve_replication_stale_params(
            {"thresholds": {"replication_stale_multiplier": "soon"}}
        )
        assert mult == 2.0

    def test_grace_negative_falls_back(self):
        _, grace = resolve_replication_stale_params(
            {"thresholds": {"replication_stale_grace_minutes": -1}}
        )
        assert grace == 30

    def test_valid_values_win(self):
        mult, grace = resolve_replication_stale_params({
            "thresholds": {
                "replication_stale_multiplier": 3.5,
                "replication_stale_grace_minutes": 60,
            }
        })
        assert mult == 3.5
        assert grace == 60


class TestLoadConfigSources:
    """Phase 3: `load_config()` always returns a `sources` list.

    Covers the synthetic-default fallback for legacy configs, pass-through of
    an explicit `sources` list, and the validator's error being re-raised
    with the config path appended to the message.
    """

    def _write(self, tmp_path, data):
        (tmp_path / "config.json").write_text(json.dumps(data))

    def test_legacy_config_without_sources_gets_synthetic_default(self, tmp_path, monkeypatch):
        monkeypatch.setattr(alert_monitor, "CONFIG_PATH", tmp_path / "config.json")
        self._write(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "x"})

        cfg = alert_monitor.load_config()
        assert cfg["sources"] == [{
            "id": "default",
            "type": "remote_backups",
            "api_base": "https://api.example.com",
            "api_key_env": "API_KEY",
        }]

    def test_config_with_sources_list_is_preserved(self, tmp_path, monkeypatch):
        monkeypatch.setattr(alert_monitor, "CONFIG_PATH", tmp_path / "config.json")
        self._write(tmp_path, {
            "api_base": "https://api.example.com",
            "ntfy_topic": "x",
            "sources": [
                {"id": "cloud-a", "type": "remote_backups", "api_base": "https://a"},
                {"id": "cloud-b", "type": "remote_backups", "api_base": "https://b"},
            ],
        })

        cfg = alert_monitor.load_config()
        assert [s["id"] for s in cfg["sources"]] == ["cloud-a", "cloud-b"]
        assert cfg["sources"][0]["api_key_env"] == "API_KEY"

    def test_invalid_source_id_raises_with_config_path_in_message(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.json"
        monkeypatch.setattr(alert_monitor, "CONFIG_PATH", config_path)
        self._write(tmp_path, {
            "api_base": "https://api.example.com",
            "sources": [
                {"id": "BadID", "type": "remote_backups", "api_base": "https://a"},
            ],
        })

        with pytest.raises(ValueError, match="Invalid 'sources'") as exc:
            alert_monitor.load_config()
        assert str(config_path) in str(exc.value)

    def test_auto_migrate_preserves_user_sources(self, tmp_path, monkeypatch):
        """Non-empty user `sources` must survive the shallow-merge auto-migrate."""
        monkeypatch.setattr(alert_monitor, "CONFIG_PATH", tmp_path / "config.json")
        user_sources = [
            {"id": "cloud-a", "type": "remote_backups", "api_base": "https://a"},
        ]
        self._write(tmp_path, {
            "api_base": "https://api.example.com",
            "sources": user_sources,
        })

        cfg = alert_monitor.load_config()
        assert cfg["sources"][0]["id"] == "cloud-a"
        # And the on-disk file still has the user's sources (auto-migrate
        # must not clobber a user-populated list).
        on_disk = json.loads((tmp_path / "config.json").read_text())
        assert on_disk["sources"] == user_sources
