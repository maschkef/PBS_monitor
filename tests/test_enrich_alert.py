"""Enrichment entry point: alerting.status_feed.enrich_alert."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from alerting import status_feed
from alerting.schedule import Alert

FIXTURE = Path(__file__).parent / "fixtures" / "status_feed.json"


@pytest.fixture
def feed_dict():
    with FIXTURE.open() as f:
        return json.load(f)


@pytest.fixture
def stub_fetch(monkeypatch, feed_dict):
    """Monkeypatch fetch_status_feed to return the fixture, no HTTP."""
    def _ret(*args, **kwargs):
        return feed_dict
    monkeypatch.setattr(status_feed, "fetch_status_feed", _ret)


def _alert(**kwargs):
    kwargs.setdefault("datastore_name", "demo")
    kwargs.setdefault("title", "Missed Backup Window")
    kwargs.setdefault("message", "backup overdue")
    return Alert(**kwargs)


def test_enrich_disabled_leaves_message(stub_fetch):
    alert = _alert()
    cfg = {"status_feed": {"enabled": False, "default_host": "dus1-1.pbs-host.de"}}
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    assert alert.message == "backup overdue"


def test_enrich_skips_non_allowlist_alerts(stub_fetch):
    alert = _alert(title="Garbage Collection Failed")
    cfg = {"status_feed": {"enabled": True, "default_host": "dus1-1.pbs-host.de"}}
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    assert alert.message == "backup overdue"


def test_enrich_appends_for_matching_host(stub_fetch):
    alert = _alert(datastore_id="ds-1", namespace="")
    cfg = {
        "status_feed": {"enabled": True, "default_host": "dus1-1.pbs-host.de"},
        "host_overrides": [],
    }
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    assert "Possibly related platform incident" in alert.message
    assert "dus1-1.pbs-host.de" in alert.message
    assert "status-feed" in alert.tags


def test_enrich_global_alert_uses_ongoing_without_host(stub_fetch):
    alert = _alert(title="API Unreachable", message="network down")
    cfg = {"status_feed": {"enabled": True, "default_host": ""}}
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    # Global degradation + dus1 are both ongoing — enrichment must trigger.
    assert "Possibly related platform incident" in alert.message
    assert "Global API performance degradation" in alert.message


def test_enrich_no_host_configured_leaves_alert(stub_fetch):
    alert = _alert()  # Missed Backup Window needs a host
    cfg = {"status_feed": {"enabled": True, "default_host": ""}}
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    assert alert.message == "backup overdue"


def test_enrich_swallows_fetch_exception(monkeypatch):
    def _boom(*args, **kwargs):
        raise RuntimeError("boom")
    monkeypatch.setattr(status_feed, "fetch_status_feed", _boom)

    alert = _alert()
    cfg = {"status_feed": {"enabled": True, "default_host": "dus1-1.pbs-host.de"}}
    # Must not raise.
    status_feed.enrich_alert(alert, cfg,
                              event_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC))
    assert alert.message == "backup overdue"


def test_resolve_host_priority_namespace_wins_over_datastore_default():
    cfg = {
        "status_feed": {"default_host": "default.example"},
        "host_overrides": [
            {"datastore_id": "ds-1", "namespace": "", "host": "ds-default.example"},
            {"datastore_id": "ds-1", "namespace": "ns-a", "host": "ns-a.example"},
        ],
    }
    assert status_feed.resolve_host_for_alert(
        cfg, datastore_id="ds-1", namespace="ns-a") == "ns-a.example"
    assert status_feed.resolve_host_for_alert(
        cfg, datastore_id="ds-1", namespace="ns-b") == "ds-default.example"
    assert status_feed.resolve_host_for_alert(
        cfg, datastore_id="ds-2", namespace="x") == "default.example"


def test_resolve_host_returns_none_when_no_config():
    assert status_feed.resolve_host_for_alert({}, datastore_id="ds-1") is None
