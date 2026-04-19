"""Tests for secret redaction in the alerting config API.

Scenarios covered
-----------------
Unit tests for ``_redact_config``:
  - Token present → replaced with sentinel; ``ntfy_token_set = True``.
  - Token absent  → not replaced;         ``ntfy_token_set = False``.
  - Original dict is not mutated.

Integration tests via ``GET /api/alerting/config``:
  - When a token is stored, the response carries the sentinel not the real value.
  - When no token is stored, sentinal is absent and ``ntfy_token_set = False``.

``POST /api/alerting/config`` — sentinel round-trip:
  - Submitting the sentinel back does NOT overwrite the previously stored token.
  - Submitting an empty string DOES clear the token.
  - Submitting a new real value DOES update the token.
"""
import json
import pytest

from tests.conftest import TEST_PASSWORD, do_login, write_config, read_config
import webui.app as webapp

SENTINEL = webapp._TOKEN_SENTINEL


# ── Unit tests for _redact_config ─────────────────────────────────────────────

class TestRedactConfigUnit:
    def test_token_present_is_replaced_with_sentinel(self):
        cfg = {"ntfy_url": "https://ntfy.sh", "ntfy_topic": "test", "ntfy_token": "secret123"}
        result = webapp._redact_config(cfg)
        assert result["ntfy_token"] == SENTINEL
        assert result["ntfy_token_set"] is True

    def test_token_absent_leaves_ntfy_token_set_false(self):
        cfg = {"ntfy_url": "https://ntfy.sh", "ntfy_topic": "test", "ntfy_token": ""}
        result = webapp._redact_config(cfg)
        assert result["ntfy_token"] == ""
        assert result["ntfy_token_set"] is False

    def test_missing_token_key_leaves_ntfy_token_set_false(self):
        cfg = {"ntfy_url": "https://ntfy.sh"}
        result = webapp._redact_config(cfg)
        assert result["ntfy_token_set"] is False

    def test_original_dict_is_not_mutated(self):
        original = {"ntfy_token": "real_secret"}
        webapp._redact_config(original)
        assert original["ntfy_token"] == "real_secret"


# ── Integration: GET /api/alerting/config ─────────────────────────────────────

class TestConfigGetRedaction:
    def test_stored_token_is_redacted_in_response(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"ntfy_url": "https://ntfy.sh", "ntfy_token": "super_secret"})
        rv = client_no_auth.get("/api/alerting/config")
        assert rv.status_code == 200
        body = rv.get_json()
        assert body["config"]["ntfy_token"] == SENTINEL
        assert body["config"]["ntfy_token_set"] is True
        # The real secret must not appear anywhere in the raw response bytes.
        assert b"super_secret" not in rv.data

    def test_missing_token_returns_ntfy_token_set_false(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"ntfy_url": "https://ntfy.sh", "ntfy_token": ""})
        rv = client_no_auth.get("/api/alerting/config")
        body = rv.get_json()
        assert body["config"]["ntfy_token_set"] is False

    def test_no_config_file_returns_defaults_without_token(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        # Do not write any config file.
        rv = client_no_auth.get("/api/alerting/config")
        assert rv.status_code == 200
        body = rv.get_json()
        assert body["config"]["ntfy_token_set"] is False


# ── Integration: POST sentinel round-trip ────────────────────────────────────

class TestSentinelRoundTrip:
    def _auth_headers(self, client, tmp_path, monkeypatch):
        """Login and return CSRF header dict."""
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        csrf = do_login(client, TEST_PASSWORD)
        return {"X-CSRF-Token": csrf}

    def test_submitting_sentinel_preserves_stored_token(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"ntfy_token": "original_token"})
        headers = self._auth_headers(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_token": SENTINEL},
            headers=headers,
        )
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert saved.get("ntfy_token") == "original_token"

    def test_submitting_empty_string_clears_stored_token(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"ntfy_token": "original_token"})
        headers = self._auth_headers(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_token": ""},
            headers=headers,
        )
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert saved.get("ntfy_token") == ""

    def test_submitting_new_token_updates_stored_value(self, monkeypatch, client_auth, tmp_path):
        write_config(tmp_path, {"ntfy_token": "old_token"})
        headers = self._auth_headers(client_auth, tmp_path, monkeypatch)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_token": "brand_new_token"},
            headers=headers,
        )
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert saved["ntfy_token"] == "brand_new_token"
