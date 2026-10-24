"""Unit tests for the alerting UI bridge helpers.

These tests do not exercise Flask routes; they verify that
``load_visual_alerting_config`` validates sources and that
``build_visual_alerting`` wires adapters correctly for inventory fetching.
"""

from pathlib import Path

import pytest

from alerting import monitor as alert_monitor
from webui import alerting_ui


def _state_path(tmp_path: Path) -> Path:
    return tmp_path / "state.json"


def _default_visual_payload():
    return {
        "health": "healthy",
        "alerts": [],
        "alert_count": 0,
        "max_priority": 0,
        "backup_status": {},
        "inventory_error": None,
        "state_source": "ephemeral",
        "rules_source": "ephemeral",
        "ignored_groups": [],
        "schedule_learning": {},
    }


class TestLoadVisualAlertingConfig:
    def test_valid_sources_are_normalised(self, tmp_path):
        config_path = tmp_path / "config.json"
        config_path.write_text('{"sources": [{"id": "a", "type": "remote_backups", "api_base": "https://x"}]}')
        cfg = alerting_ui.load_visual_alerting_config(config_path)
        assert cfg["sources"] == [{
            "id": "a",
            "type": "remote_backups",
            "api_base": "https://x",
            "api_key_env": "API_KEY",
        }]

    def test_invalid_sources_raises_value_error(self, tmp_path):
        config_path = tmp_path / "config.json"
        config_path.write_text('{"sources": [{"id": "a", "type": "remote_backups"}]}')
        with pytest.raises(ValueError):
            alerting_ui.load_visual_alerting_config(config_path)

    def test_missing_config_and_legacy_api_base_synthesises_default(self, tmp_path):
        config_path = tmp_path / "config.json"
        cfg = alerting_ui.load_visual_alerting_config(config_path)
        assert cfg["sources"][0]["type"] == "remote_backups"
        assert cfg["sources"][0]["id"] == "default"


class TestBuildVisualAlerting:
    def test_adapter_none_skips_inventory_fetch(self, monkeypatch, tmp_path):
        captured = {}

        def fake_check_datastore(detail, config, state, *, backup_inventory=None, **kwargs):
            captured["backup_inventory"] = backup_inventory
            return [], {}

        monkeypatch.setattr(alert_monitor, "check_datastore", fake_check_datastore)
        monkeypatch.setattr(alerting_ui, "collect_schedule_groups", lambda *a, **k: [])

        result = alerting_ui.build_visual_alerting(
            {"id": "ds1", "metrics": {"used_percent": 10}},
            alert_monitor.DEFAULT_CONFIG,
            alert_monitor.default_state(),
            {"rules": {}},
            "ephemeral",
            _state_path(tmp_path),
            adapter=None,
            fetch_inventory=True,
        )

        assert captured["backup_inventory"] is None
        assert result["inventory_error"] is None

    def test_adapter_provided_fetches_inventory_with_snapshot_cap(self, monkeypatch, tmp_path):
        class FakeAdapter:
            source_id = "fake"

            def __init__(self):
                self.calls = []

            def fetch_backup_inventory(self, ds_id, *, snapshot_cap):
                self.calls.append((ds_id, snapshot_cap))
                return {"summary": {"group_count": 0}, "groups": []}

        captured = {}

        def fake_check_datastore(detail, config, state, *, backup_inventory=None, **kwargs):
            captured["backup_inventory"] = backup_inventory
            return [], {}

        monkeypatch.setattr(alert_monitor, "check_datastore", fake_check_datastore)
        monkeypatch.setattr(alerting_ui, "collect_schedule_groups", lambda *a, **k: [])

        adapter = FakeAdapter()
        config = {
            **alert_monitor.DEFAULT_CONFIG,
            "schedule_learning": {"snapshot_retention_count": 7},
        }
        alerting_ui.build_visual_alerting(
            {"id": "ds1", "metrics": {"used_percent": 10}},
            config,
            alert_monitor.default_state(),
            {"rules": {}},
            "ephemeral",
            _state_path(tmp_path),
            adapter=adapter,
            fetch_inventory=True,
        )

        assert adapter.calls == [("ds1", 7)]
        assert captured["backup_inventory"] == {"summary": {"group_count": 0}, "groups": []}

    def test_source_connection_error_becomes_inventory_error(self, monkeypatch, tmp_path):
        from alerting.sources import SourceConnectionError

        class BrokenAdapter:
            source_id = "broken"

            def fetch_backup_inventory(self, ds_id, *, snapshot_cap):
                raise SourceConnectionError("TLS failure")

        monkeypatch.setattr(alert_monitor, "check_datastore", lambda *a, **kw: ([], {}))
        monkeypatch.setattr(alerting_ui, "collect_schedule_groups", lambda *a, **k: [])

        result = alerting_ui.build_visual_alerting(
            {"id": "ds1", "metrics": {"used_percent": 10}},
            alert_monitor.DEFAULT_CONFIG,
            alert_monitor.default_state(),
            {"rules": {}},
            "ephemeral",
            _state_path(tmp_path),
            adapter=BrokenAdapter(),
            fetch_inventory=True,
        )

        assert "TLS failure" in (result.get("inventory_error") or "")
