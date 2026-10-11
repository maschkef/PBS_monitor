"""Parsing & matching tests for alerting.status_feed."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from alerting import status_feed

FIXTURE = Path(__file__).parent / "fixtures" / "status_feed.json"


@pytest.fixture
def feed():
    with FIXTURE.open() as f:
        return json.load(f)


def test_index_resources_builds_both_maps(feed):
    name_to_id, id_to_name = status_feed.index_resources(feed)
    assert name_to_id["dus1-1.pbs-host.de"] == "100"
    assert name_to_id["fra1-ingress.pbs-host.de"] == "101"
    assert id_to_name["100"] == "dus1-1.pbs-host.de"
    assert id_to_name["102"] == "hil1-1.pbs-host.de"


def test_index_resources_is_case_insensitive_on_name(feed):
    name_to_id, _ = status_feed.index_resources(feed)
    # keys are lowercased
    assert "dus1-1.pbs-host.de" in name_to_id
    assert "DUS1-1.PBS-HOST.DE".lower() in name_to_id


def test_find_matching_incidents_host_scope_returns_only_matching_host(feed):
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(
        feed, host="dus1-1.pbs-host.de", at_time=at, lookback_minutes=120,
    )
    assert len(hits) == 1
    assert hits[0]["is_ongoing"] is True
    assert hits[0]["matched_host"] == "dus1-1.pbs-host.de"
    assert "dus1-1.pbs-host.de" in hits[0]["affected_hosts"]


def test_find_matching_incidents_host_scope_filters_old_resolved(feed):
    # fra1 maintenance was yesterday; a small lookback window must not include it.
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(
        feed, host="fra1-ingress.pbs-host.de", at_time=at, lookback_minutes=120,
    )
    assert hits == []


def test_find_matching_incidents_host_scope_keeps_resolved_within_lookback(feed):
    # Looking back 2 days should surface the resolved maintenance.
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(
        feed, host="fra1-ingress.pbs-host.de", at_time=at, lookback_minutes=60 * 48,
    )
    assert len(hits) == 1
    assert hits[0]["is_ongoing"] is False
    assert hits[0]["aggregate_state"] == "resolved"


def test_find_matching_incidents_global_scope_only_ongoing(feed):
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(feed, host=None, at_time=at)
    titles = {h["title"] for h in hits}
    # Only ongoing incidents: dus1 outage + global degradation
    assert "Storage node dus1-1 unreachable" in titles
    assert "Global API performance degradation" in titles
    assert "Scheduled maintenance on fra1-ingress" not in titles


def test_find_matching_incidents_unknown_host_returns_empty(feed):
    hits = status_feed.find_matching_incidents(
        feed, host="nope.example.com", at_time=datetime.now(UTC),
    )
    assert hits == []


def test_host_matches_public_name_rules():
    assert status_feed.host_matches_public_name(
        "ber1-ingress.pbs-host.de", "ber1-ingress.pbs-host.de - Instance 1",
    )
    assert status_feed.host_matches_public_name(
        "ber1-ingress.pbs-host.de", "ber1-ingress.pbs-host.de",
    )
    # Case + whitespace tolerant.
    assert status_feed.host_matches_public_name(
        "  BER1-INGRESS.pbs-host.de  ", "ber1-ingress.pbs-host.de - Instance 2",
    )
    # Must not match a different host that happens to share the prefix.
    assert not status_feed.host_matches_public_name(
        "ber1-ingress.pbs-host.de", "ber1-ingress-extra.pbs-host.de",
    )
    # Empty values do not match anything.
    assert not status_feed.host_matches_public_name("", "any.example.com")
    assert not status_feed.host_matches_public_name("any.example.com", "")


def test_find_matching_incidents_matches_multi_instance_resources(feed):
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(
        feed, host="ber1-ingress.pbs-host.de", at_time=at, lookback_minutes=180,
    )
    # Incident 503 affects "ber1-ingress.pbs-host.de - Instance 2" and must
    # be surfaced when the user configures the base host.
    assert len(hits) == 1
    assert hits[0]["title"] == "ber1-ingress Instance 2 degraded"
    assert hits[0]["matched_host"] == "ber1-ingress.pbs-host.de - Instance 2"


def test_find_matching_incidents_does_not_match_prefix_lookalike(feed):
    # ber1-ingress-extra.pbs-host.de shares a prefix with ber1-ingress.pbs-host.de
    # but is a distinct resource; it must not pick up the Instance 2 incident.
    at = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    hits = status_feed.find_matching_incidents(
        feed, host="ber1-ingress-extra.pbs-host.de", at_time=at, lookback_minutes=180,
    )
    assert hits == []


def test_find_matching_incidents_sort_ongoing_first(feed):
    hits = status_feed.find_matching_incidents(feed, host=None, at_time=datetime.now(UTC))
    assert all(h["is_ongoing"] for h in hits)


def test_parse_iso_handles_z_suffix():
    dt = status_feed.parse_iso("2026-10-11T12:00:00Z")
    # Python 3.11+ accepts Z via fromisoformat.
    assert dt is not None
    assert dt.year == 2026


def test_parse_iso_returns_none_on_garbage():
    assert status_feed.parse_iso(None) is None
    assert status_feed.parse_iso("not a date") is None
    assert status_feed.parse_iso("") is None


def test_format_enrichment_text_empty_is_empty_string():
    assert status_feed.format_enrichment_text([]) == ""


def test_format_enrichment_text_contains_prefix_and_marker(feed):
    hits = status_feed.find_matching_incidents(
        feed, host="dus1-1.pbs-host.de",
        at_time=datetime(2026, 10, 11, 12, 0, tzinfo=UTC),
    )
    text = status_feed.format_enrichment_text(hits)
    assert "Possibly related platform incident" in text
    assert "ONGOING" in text
    assert "dus1-1.pbs-host.de" in text
