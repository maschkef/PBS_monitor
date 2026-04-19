"""Tests for HTTP security headers injected by the ``add_security_headers`` hook.

Every response — regardless of route, method, or auth state — must carry the
full set of defensive headers.

Headers verified
----------------
- ``X-Content-Type-Options: nosniff``
- ``X-Frame-Options: DENY``
- ``Referrer-Policy: strict-origin-when-cross-origin``
- ``Content-Security-Policy`` (presence and key directives)
"""
import pytest

import webui.app as webapp
from tests.conftest import TEST_PASSWORD, do_login


REQUIRED_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
}

CSP_REQUIRED_DIRECTIVES = [
    "default-src 'self'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'none'",
]


def _assert_security_headers(response) -> None:
    """Assert that *response* contains all required security headers."""
    for header, expected_value in REQUIRED_HEADERS.items():
        actual = response.headers.get(header, "")
        assert actual == expected_value, (
            f"Header '{header}': expected '{expected_value}', got '{actual}'"
        )

    csp = response.headers.get("Content-Security-Policy", "")
    assert csp, "Content-Security-Policy header is missing"
    for directive in CSP_REQUIRED_DIRECTIVES:
        assert directive in csp, (
            f"CSP directive '{directive}' missing from: {csp}"
        )


class TestSecurityHeadersNoAuth:
    def test_index_has_security_headers(self, client_no_auth):
        rv = client_no_auth.get("/")
        _assert_security_headers(rv)

    def test_api_config_get_has_security_headers(self, monkeypatch, client_no_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        rv = client_no_auth.get("/api/alerting/config")
        _assert_security_headers(rv)

    def test_404_response_has_security_headers(self, client_no_auth):
        rv = client_no_auth.get("/this-route-does-not-exist")
        _assert_security_headers(rv)

    def test_login_redirect_has_security_headers(self, monkeypatch, client_no_auth):
        # With auth disabled, /login redirects to /. The redirect response
        # should still carry security headers.
        rv = client_no_auth.get("/login", follow_redirects=False)
        _assert_security_headers(rv)


class TestSecurityHeadersWithAuth:
    def test_login_page_has_security_headers(self, client_auth):
        rv = client_auth.get("/login")
        _assert_security_headers(rv)

    def test_unauthenticated_api_401_has_security_headers(self, client_auth):
        rv = client_auth.get("/api/alerting/config")
        assert rv.status_code == 401
        _assert_security_headers(rv)

    def test_unauthenticated_get_redirect_has_security_headers(self, client_auth):
        rv = client_auth.get("/", follow_redirects=False)
        assert rv.status_code == 302
        _assert_security_headers(rv)

    def test_authenticated_index_has_security_headers(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.get("/")
        _assert_security_headers(rv)

    def test_csrf_403_response_has_security_headers(self, client_auth):
        do_login(client_auth, TEST_PASSWORD)
        # POST without CSRF token → 403.
        rv = client_auth.post("/api/alerting/config", json={})
        assert rv.status_code == 403
        _assert_security_headers(rv)

    def test_rate_limit_429_has_security_headers(self, monkeypatch, client_auth, tmp_path):
        """The global 429 error handler response must carry security headers.

        The /login endpoint has a 10/minute limit. We temporarily re-enable the
        limiter, exhaust the limit, verify the 429 carries headers, then the
        next fixture will call limiter.reset() to clean up.
        """
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        webapp.app.config["RATELIMIT_ENABLED"] = True
        webapp.limiter.reset()
        try:
            rv_429 = None
            for _ in range(12):
                rv = client_auth.get("/login")
                if rv.status_code == 429:
                    rv_429 = rv
                    break
        finally:
            webapp.app.config["RATELIMIT_ENABLED"] = False
            webapp.limiter.reset()

        assert rv_429 is not None, "Expected a 429 response but never got one"
        _assert_security_headers(rv_429)
