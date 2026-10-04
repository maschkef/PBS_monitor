"""Tests for the config-value resolver helpers in alerting.monitor."""

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
