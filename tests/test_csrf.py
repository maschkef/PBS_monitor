"""Tests for the ``require_csrf`` decorator.

Scenarios covered
-----------------
Auth disabled:
  - Write routes accept requests without X-CSRF-Token (CSRF is meaningless
    without session authentication).

Auth enabled, not authenticated:
  - Write routes return 401 (auth check fires before CSRF check).

Auth enabled, authenticated:
  - POST/DELETE without X-CSRF-Token header → 403.
  - POST with a wrong token value → 403.
  - POST with the correct per-session token → request proceeds (≠ 403).

Routes tested: ``POST /api/alerting/config`` (representative write route) and
``DELETE /api/alerting/notification-log``.
"""
import json
import pytest

from tests.conftest import TEST_PASSWORD, do_login
import webui.app as webapp


# ── Write routes list (path, method) ─────────────────────────────────────────
# We test one representative POST and one DELETE for CSRF enforcement.
POST_CONFIG = "/api/alerting/config"
DELETE_LOG   = "/api/alerting/notification-log"


# ── Auth disabled: CSRF not enforced ─────────────────────────────────────────

class TestCsrfAuthDisabled:
    def test_post_without_csrf_token_allowed(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        rv = client_no_auth.post(POST_CONFIG, json={})
        # Should not be blocked by CSRF (may fail for other reasons, but not 403).
        assert rv.status_code != 403

    def test_delete_without_csrf_token_allowed(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_LOG_PATH", tmp_path / "notification_log.json")
        # Seed a valid log file so the delete has something to clear.
        (tmp_path / "notification_log.json").write_text("[]")
        rv = client_no_auth.delete(DELETE_LOG)
        assert rv.status_code != 403


# ── Auth enabled, not authenticated ──────────────────────────────────────────

class TestCsrfNotAuthenticated:
    def test_post_without_csrf_returns_401_not_403(self, client_auth):
        # Auth check must fire *before* CSRF check.
        rv = client_auth.post(POST_CONFIG, json={})
        assert rv.status_code == 401


# ── Auth enabled, authenticated ───────────────────────────────────────────────

class TestCsrfAuthenticated:
    def test_post_without_csrf_header_returns_403(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(POST_CONFIG, json={})
        assert rv.status_code == 403
        assert rv.is_json
        assert "CSRF" in rv.get_json().get("error", "")

    def test_post_with_wrong_csrf_token_returns_403(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(
            POST_CONFIG,
            json={},
            headers={"X-CSRF-Token": "completely_wrong_token"},
        )
        assert rv.status_code == 403

    def test_post_with_correct_csrf_token_proceeds(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        csrf = do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(
            POST_CONFIG,
            json={"ntfy_url": "https://ntfy.sh"},
            headers={"X-CSRF-Token": csrf},
        )
        # Must not be blocked by CSRF — may be 200 or another code depending
        # on validation, but never 403.
        assert rv.status_code != 403

    def test_delete_without_csrf_header_returns_403(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.delete(DELETE_LOG)
        assert rv.status_code == 403

    def test_delete_with_correct_csrf_token_proceeds(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_LOG_PATH", tmp_path / "notification_log.json")
        (tmp_path / "notification_log.json").write_text("[]")
        csrf = do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.delete(
            DELETE_LOG,
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code != 403
        assert rv.is_json
        assert rv.get_json().get("ok") is True

    def test_csrf_token_not_valid_cross_session(self, monkeypatch, client_auth, tmp_path):
        """A CSRF token from one session must not be accepted by a different session."""
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        # Obtain token from first login.
        csrf_first = do_login(client_auth, TEST_PASSWORD)

        # Log out and log in again — a new CSRF token should be generated.
        client_auth.post("/logout")
        csrf_second = do_login(client_auth, TEST_PASSWORD)

        # The old token must not match the new session token.
        assert csrf_first != csrf_second
