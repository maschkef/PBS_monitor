"""Shared pytest fixtures for PBS Monitor test suite.

Isolation strategy
------------------
All tests patch module-level singletons in ``webui.app`` via ``monkeypatch``
so that each fixture variant  (auth on / off) is independent of shell
environment variables and real on-disk files.

Rate limiting (flask-limiter) is disabled globally via ``RATELIMIT_ENABLED``
to keep tests deterministic.
"""
import json
import sys
from pathlib import Path

import pytest

# Ensure the project root is importable regardless of where pytest is invoked.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import webui.app as webapp  # noqa: E402  (must come after sys.path setup)

# Password used for all auth-enabled fixtures.
TEST_PASSWORD = "Test_P@ssword_42!"


def _base_app_config(monkeypatch, tmp_path: Path, *, password: str) -> None:
    """Apply common test-time patches to the ``webapp`` module."""
    monkeypatch.setattr(webapp, "WEBUI_PASSWORD", password)
    monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(webapp, "ALERTING_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(webapp, "ALERTING_LOG_PATH", tmp_path / "notification_log.json")
    monkeypatch.setattr(webapp, "WEBUI_READ_ONLY", False)
    webapp.app.config["TESTING"] = True
    # Disable rate limiting so tests are not throttled.
    webapp.app.config["RATELIMIT_ENABLED"] = False
    # Reset in-memory counters from any previous test so the per-route limits
    # (e.g. 10/min on /login) do not bleed across test boundaries.
    webapp.limiter.reset()
    # Stable secret key avoids session warnings and keeps sessions valid across
    # requests within the same test.
    webapp.app.secret_key = "pytest-stable-secret-key"


# ── No-auth fixtures ──────────────────────────────────────────────────────────

@pytest.fixture()
def client_no_auth(monkeypatch, tmp_path):
    """Test client with authentication disabled (WEBUI_PASSWORD = '')."""
    _base_app_config(monkeypatch, tmp_path, password="")
    with webapp.app.test_client() as client:
        yield client


# ── Auth-enabled fixtures ─────────────────────────────────────────────────────

@pytest.fixture()
def client_auth(monkeypatch, tmp_path):
    """Test client with authentication enabled; not yet logged in."""
    _base_app_config(monkeypatch, tmp_path, password=TEST_PASSWORD)
    with webapp.app.test_client() as client:
        yield client


@pytest.fixture()
def client_logged_in(client_auth):
    """Test client already authenticated; returns ``(client, csrf_token)``."""
    csrf = do_login(client_auth, TEST_PASSWORD)
    return client_auth, csrf


# ── Helpers (also importable by individual test modules) ─────────────────────

def do_login(client, password: str = TEST_PASSWORD) -> str:
    """Perform the full two-step login and return the session CSRF token.

    1. GET /login  → server plants a per-form nonce in the session.
    2. POST /login → submit matching nonce + password.
    Returns the CSRF token stored in the session after successful login.
    Raises AssertionError if login does not result in a redirect (302).
    """
    rv = client.get("/login")
    assert rv.status_code == 200, f"GET /login returned {rv.status_code}"

    with client.session_transaction() as sess:
        nonce = sess.get("login_nonce", "")
    assert nonce, "No login nonce planted by GET /login"

    rv = client.post(
        "/login",
        data={"password": password, "_nonce": nonce},
        follow_redirects=False,
    )
    assert rv.status_code == 302, f"POST /login returned {rv.status_code}, expected redirect"

    with client.session_transaction() as sess:
        csrf = sess.get("csrf_token", "")
    assert csrf, "No CSRF token in session after login"
    return csrf


def write_config(tmp_path: Path, data: dict) -> None:
    """Write *data* as JSON to the standard test alerting config path."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(data))


def read_config(tmp_path: Path) -> dict:
    """Read the test alerting config file."""
    config_path = tmp_path / "config.json"
    return json.loads(config_path.read_text())
