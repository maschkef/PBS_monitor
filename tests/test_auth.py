"""Tests for the ``require_auth`` decorator and the login/logout flow.

Scenarios covered
-----------------
Auth disabled (WEBUI_PASSWORD = ''):
  - All GET routes are accessible without a session.
  - All write API routes are accessible without a session.

Auth enabled (WEBUI_PASSWORD set):
  - GET /  redirects to /login when not authenticated.
  - GET /api/* returns 401 JSON when not authenticated.
  - /login GET renders the form (200).
  - POST /login with correct password + valid nonce → redirect to /, sets session.
  - POST /login with wrong password → 200 with error (no redirect).
  - POST /login with correct password but wrong nonce → 200 with error.
  - Authenticated session allows access to / (200).
  - POST /logout clears session and redirects.
"""
import pytest

from tests.conftest import TEST_PASSWORD, do_login
import webui.app as webapp


# ── Auth disabled ─────────────────────────────────────────────────────────────

class TestAuthDisabled:
    def test_index_accessible_without_session(self, client_no_auth):
        rv = client_no_auth.get("/")
        assert rv.status_code == 200

    def test_api_alerting_config_accessible_without_session(self, monkeypatch, client_no_auth, tmp_path):
        # Config GET should work with no auth and return JSON.
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        rv = client_no_auth.get("/api/alerting/config")
        assert rv.status_code == 200
        assert rv.is_json

    def test_login_route_redirects_to_index(self, client_no_auth):
        rv = client_no_auth.get("/login", follow_redirects=False)
        assert rv.status_code == 302
        assert rv.location.endswith("/")


# ── Auth enabled ──────────────────────────────────────────────────────────────

class TestAuthEnabled:
    def test_get_index_redirects_to_login(self, client_auth):
        rv = client_auth.get("/", follow_redirects=False)
        assert rv.status_code == 302
        assert "/login" in rv.location

    def test_api_route_returns_401_when_not_authenticated(self, client_auth):
        rv = client_auth.get("/api/alerting/config")
        assert rv.status_code == 401
        assert rv.is_json
        data = rv.get_json()
        assert "error" in data

    def test_api_write_returns_401_when_not_authenticated(self, client_auth):
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_url": "https://ntfy.sh"},
        )
        assert rv.status_code == 401

    def test_login_page_returns_200(self, client_auth):
        rv = client_auth.get("/login")
        assert rv.status_code == 200

    def test_login_success_redirects_and_sets_session(self, client_auth):
        csrf = do_login(client_auth, TEST_PASSWORD)
        assert csrf  # session has a CSRF token after successful login

        # / is now accessible.
        rv = client_auth.get("/")
        assert rv.status_code == 200

    def test_login_wrong_password_returns_200_with_error(self, client_auth):
        rv = client_auth.get("/login")
        with client_auth.session_transaction() as sess:
            nonce = sess["login_nonce"]
        rv = client_auth.post(
            "/login",
            data={"password": "wrongpassword", "_nonce": nonce},
            follow_redirects=False,
        )
        # Must stay on login page, not redirect.
        assert rv.status_code == 200
        assert b"Invalid password" in rv.data

    def test_login_wrong_nonce_returns_200_with_error(self, client_auth):
        rv = client_auth.get("/login")
        rv = client_auth.post(
            "/login",
            data={"password": TEST_PASSWORD, "_nonce": "definitely_wrong_nonce"},
            follow_redirects=False,
        )
        assert rv.status_code == 200
        assert b"Invalid password" in rv.data

    def test_authenticated_session_allows_api_access(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.get("/api/alerting/config")
        assert rv.status_code == 200
        assert rv.is_json

    def test_logout_clears_session(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        # Should be logged in now.
        with client_auth.session_transaction() as sess:
            assert sess.get("authenticated")

        rv = client_auth.post("/logout", follow_redirects=False)
        assert rv.status_code == 302

        with client_auth.session_transaction() as sess:
            assert not sess.get("authenticated")

    def test_post_to_protected_route_after_logout_returns_401(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        client_auth.post("/logout")
        rv = client_auth.post(
            "/api/alerting/config",
            json={"ntfy_url": "https://ntfy.sh"},
        )
        assert rv.status_code == 401
