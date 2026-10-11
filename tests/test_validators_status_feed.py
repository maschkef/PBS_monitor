"""Validator tests for status_feed + host_overrides sections of the config payload."""
from __future__ import annotations

import pytest

import webui.app as webapp
from tests.conftest import TEST_PASSWORD, do_login


def _setup(client, tmp_path, monkeypatch):
    monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
    return do_login(client, TEST_PASSWORD)


def _post(client, csrf, payload):
    return client.post(
        "/api/alerting/config",
        json=payload,
        headers={"X-CSRF-Token": csrf},
    )


class TestStatusFeedBounds:
    def test_status_feed_must_be_object(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"status_feed": "nope"})
        assert rv.status_code == 400
        assert "status_feed" in rv.get_json().get("error", "")

    def test_status_feed_enabled_must_be_bool(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"status_feed": {"enabled": "yes"}})
        assert rv.status_code == 400

    def test_status_feed_url_must_be_http(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf,
                   {"status_feed": {"url": "ftp://example.com/feed.json"}})
        assert rv.status_code == 400
        assert "status_feed.url" in rv.get_json().get("error", "")

    @pytest.mark.parametrize("field,value", [
        ("cache_minutes", 0),
        ("cache_minutes", 10000),
        ("lookback_minutes", 1),
        ("lookback_minutes", 999999),
    ])
    def test_status_feed_bounds(self, monkeypatch, client_auth, tmp_path, field, value):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"status_feed": {field: value}})
        assert rv.status_code == 400
        assert field in rv.get_json().get("error", "")

    def test_status_feed_default_host_too_long(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf,
                   {"status_feed": {"default_host": "a" * 254}})
        assert rv.status_code == 400

    def test_status_feed_valid_payload_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"status_feed": {
            "enabled": True,
            "url": "https://status.example/index.json",
            "cache_minutes": 15,
            "lookback_minutes": 180,
            "default_host": "dus1-1.pbs-host.de",
        }})
        assert rv.status_code == 200


class TestHostOverrides:
    def test_host_overrides_must_be_list(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": {"nope": "nope"}})
        assert rv.status_code == 400

    def test_host_overrides_entry_must_be_object(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": ["string"]})
        assert rv.status_code == 400

    def test_host_overrides_require_datastore_id(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": [
            {"datastore_id": "", "namespace": "", "host": "x.example"},
        ]})
        assert rv.status_code == 400
        assert "datastore_id" in rv.get_json().get("error", "")

    def test_host_overrides_require_host(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": [
            {"datastore_id": "ds-1", "namespace": "", "host": ""},
        ]})
        assert rv.status_code == 400

    def test_host_overrides_reject_duplicates(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": [
            {"datastore_id": "ds-1", "namespace": "ns-a", "host": "a.example"},
            {"datastore_id": "ds-1", "namespace": "ns-a", "host": "b.example"},
        ]})
        assert rv.status_code == 400
        assert "duplicate" in rv.get_json().get("error", "").lower()

    def test_host_overrides_valid_payload_accepted(self, monkeypatch, client_auth, tmp_path):
        csrf = _setup(client_auth, tmp_path, monkeypatch)
        rv = _post(client_auth, csrf, {"host_overrides": [
            {"datastore_id": "ds-1", "namespace": "", "host": "a.example"},
            {"datastore_id": "ds-1", "namespace": "ns-a", "host": "b.example"},
            {"datastore_id": "ds-2", "namespace": "", "host": "c.example"},
        ]})
        assert rv.status_code == 200
