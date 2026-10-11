"""Fetch & cache behavior of alerting.status_feed.fetch_status_feed."""
from __future__ import annotations

import requests

from alerting import status_feed


class _FakeResp:
    def __init__(self, payload=None, text=None, status=200):
        self._payload = payload
        self._text = text
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        if self._payload is None:
            raise ValueError("invalid json")
        return self._payload


def test_fetch_returns_none_on_network_error(monkeypatch):
    def _boom(url, timeout):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(status_feed.requests, "get", _boom)
    cache = {"ts": 0.0, "data": None}
    assert status_feed.fetch_status_feed(cache=cache) is None
    assert cache["data"] is None


def test_fetch_returns_none_on_bad_json(monkeypatch):
    def _ret(url, timeout):
        return _FakeResp(payload=None)

    monkeypatch.setattr(status_feed.requests, "get", _ret)
    cache = {"ts": 0.0, "data": None}
    assert status_feed.fetch_status_feed(cache=cache) is None


def test_fetch_caches_response(monkeypatch):
    calls = {"n": 0}

    def _ret(url, timeout):
        calls["n"] += 1
        return _FakeResp(payload={"data": {"id": "1"}, "included": []})

    monkeypatch.setattr(status_feed.requests, "get", _ret)
    cache = {"ts": 0.0, "data": None}
    first = status_feed.fetch_status_feed(cache=cache, cache_ttl_seconds=60)
    second = status_feed.fetch_status_feed(cache=cache, cache_ttl_seconds=60)
    assert first is not None
    assert second is first
    assert calls["n"] == 1


def test_fetch_force_refresh_bypasses_cache(monkeypatch):
    calls = {"n": 0}

    def _ret(url, timeout):
        calls["n"] += 1
        return _FakeResp(payload={"data": {"id": "1"}, "included": []})

    monkeypatch.setattr(status_feed.requests, "get", _ret)
    cache = {"ts": 0.0, "data": None}
    status_feed.fetch_status_feed(cache=cache, cache_ttl_seconds=60)
    status_feed.fetch_status_feed(cache=cache, cache_ttl_seconds=60, force_refresh=True)
    assert calls["n"] == 2
