"""Tests for /api/alerting/test/source (per-source connection probe)."""

from unittest.mock import MagicMock

import pytest
import requests

import webui.app as webapp
from alerting.sources import HealthResult, SourceConnectionError
from tests.conftest import TEST_PASSWORD, do_login


def _make_adapter(*, health=None, datastores=None, fetch_exc=None,
                  inventory=None, inventory_exc=None, source_id="src"):
    """Build a MagicMock adapter honouring the SourceAdapter protocol subset we use."""
    adapter = MagicMock()
    adapter.source_id = source_id
    adapter.check_health.return_value = health or HealthResult(status="ok", message="ok")
    if fetch_exc is not None:
        adapter.fetch_datastores.side_effect = fetch_exc
    else:
        adapter.fetch_datastores.return_value = datastores or []
    if inventory_exc is not None:
        adapter.fetch_backup_inventory.side_effect = inventory_exc
    else:
        adapter.fetch_backup_inventory.return_value = inventory or {"summary": {"snapshot_count": 0}, "groups": []}
    return adapter


def _http_error(code):
    resp = requests.Response()
    resp.status_code = code
    err = requests.HTTPError(f"{code}", response=resp)
    return err


def _get_steps(payload):
    """Return steps keyed by name for easy assertions."""
    return {s["name"]: s for s in payload["steps"]}


# ── Request-level guardrails ────────────────────────────────────────────────

class TestRequestGuardrails:
    def test_malformed_body_returns_400(self, client_logged_in):
        client, csrf = client_logged_in
        rv = client.post("/api/alerting/test/source",
                         json={}, headers={"X-CSRF-Token": csrf})
        assert rv.status_code == 400
        assert "source" in (rv.get_json() or {}).get("error", "").lower()

    def test_requires_csrf(self, client_logged_in):
        client, _ = client_logged_in
        rv = client.post("/api/alerting/test/source",
                         json={"source": {"id": "x", "type": "pbs"}})
        assert rv.status_code == 403

    def test_requires_auth(self, client_auth):
        rv = client_auth.post("/api/alerting/test/source",
                              json={"source": {}})
        assert rv.status_code in (302, 401)

    def test_allowed_in_read_only_mode(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "WEBUI_READ_ONLY", True)
        monkeypatch.setenv("API_KEY", "cloud-secret")
        monkeypatch.setattr(webapp, "build_source",
                            lambda e: _make_adapter(datastores=[{"id": "ds1", "metrics": {"backup_count": 2}}]))
        csrf = do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "cloud", "type": "remote_backups",
                "api_base": "https://api.example.com",
                "api_key_env": "API_KEY",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert rv.get_json()["ok"] is True


# ── Step 1: settings validation ─────────────────────────────────────────────

class TestSettingsStep:
    def test_unknown_type_fails_at_settings(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {"id": "x", "type": "bogus"}},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["ok"] is False
        steps = _get_steps(data)
        assert steps["settings"]["status"] == "fail"
        assert steps["reachable"]["status"] == "skipped"
        assert steps["auth"]["status"] == "skipped"
        assert steps["datastores"]["status"] == "skipped"

    def test_missing_env_var_fails_at_settings(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.delenv("NEVER_SET_TOKEN", raising=False)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "cloud", "type": "remote_backups",
                "api_base": "https://api.example.com",
                "api_key_env": "NEVER_SET_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["ok"] is False
        steps = _get_steps(data)
        assert steps["settings"]["status"] == "fail"
        assert "NEVER_SET_TOKEN" in steps["settings"]["message"]
        assert steps["reachable"]["status"] == "skipped"


# ── Step 2: reachable ───────────────────────────────────────────────────────

class TestReachableStep:
    def test_dns_failure_fails_at_reachable(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        adapter = _make_adapter(health=HealthResult(
            status="down", message="connect error",
            net_issue="DNS resolution failed for 'nope.invalid'",
        ))
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://nope.invalid:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        data = rv.get_json()
        assert data["ok"] is False
        steps = _get_steps(data)
        assert steps["reachable"]["status"] == "fail"
        assert "DNS" in steps["reachable"]["message"]
        assert steps["auth"]["status"] == "skipped"

    def test_tls_fingerprint_mismatch_fails_at_reachable(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        adapter = _make_adapter(health=HealthResult(
            status="down", message="TLS verification failed",
            error=SourceConnectionError("TLS verification failed"),
        ))
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        steps = _get_steps(rv.get_json())
        assert steps["reachable"]["status"] == "fail"
        assert "TLS" in steps["reachable"]["message"] or "unreachable" in steps["reachable"]["message"]


# ── Step 3: auth ────────────────────────────────────────────────────────────

class TestAuthStep:
    @pytest.mark.parametrize("code,expected_substring", [
        (401, "Authentication failed"),
        (403, "Datastore.Audit"),
    ])
    def test_pbs_http_error_maps_to_auth_fail(
        self, monkeypatch, client_logged_in, code, expected_substring,
    ):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        adapter = _make_adapter(
            health=HealthResult(status="ok", message="PBS 3.2"),
            fetch_exc=_http_error(code),
        )
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        data = rv.get_json()
        assert data["ok"] is False
        steps = _get_steps(data)
        assert steps["reachable"]["status"] == "ok"
        assert steps["auth"]["status"] == "fail"
        assert expected_substring in steps["auth"]["message"]
        assert steps["datastores"]["status"] == "skipped"

    def test_pbs_degraded_health_from_401_lets_auth_step_report(self, monkeypatch, client_logged_in):
        """PBS /version 401/403 → check_health returns degraded; auth step must own the message."""
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        adapter = _make_adapter(
            health=HealthResult(status="degraded", message="PBS auth failed (HTTP 401)"),
            fetch_exc=_http_error(401),
        )
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        steps = _get_steps(rv.get_json())
        # reachable collapsed to ok so auth owns the actionable message
        assert steps["reachable"]["status"] == "ok"
        assert steps["auth"]["status"] == "fail"
        assert "401" in steps["auth"]["message"]


# ── Step 4: datastores ──────────────────────────────────────────────────────

class TestDatastoresStep:
    def test_pbs_success_all_green_counts_backups(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        datastores = [
            {"id": "pbs:home:ssd", "metrics": {"backup_count": 5}},
            {"id": "pbs:home:hdd", "metrics": {"backup_count": 0}},
        ]
        adapter = _make_adapter(datastores=datastores)
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "home", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        data = rv.get_json()
        assert data["ok"] is True
        steps = _get_steps(data)
        for name in ("settings", "reachable", "auth", "datastores"):
            assert steps[name]["status"] == "ok", name
        assert "2 datastores" in steps["datastores"]["message"]
        assert "5 snapshots total" in steps["datastores"]["message"]

    def test_empty_datastores_warns(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        monkeypatch.setattr(webapp, "build_source", lambda e: _make_adapter(datastores=[]))
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        data = rv.get_json()
        assert data["ok"] is True  # warn does not fail
        steps = _get_steps(data)
        assert steps["datastores"]["status"] == "warn"
        assert "No datastores" in steps["datastores"]["message"]
        assert data["summary"] == "Connection successful with warnings"

    def test_zero_backups_warns(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("PBS_TOKEN", "u@pam!t:secret")
        datastores = [{"id": "pbs:home:ssd", "metrics": {"backup_count": 0}}]
        monkeypatch.setattr(webapp, "build_source", lambda e: _make_adapter(datastores=datastores))
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "pbs", "type": "pbs",
                "base_url": "https://pbs.local:8007",
                "node": "localhost",
                "token_env": "PBS_TOKEN",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        steps = _get_steps(rv.get_json())
        assert steps["datastores"]["status"] == "warn"
        assert "no backups yet" in steps["datastores"]["message"]

    def test_remote_backups_falls_back_to_inventory_probe(self, monkeypatch, client_logged_in):
        """When metrics.backup_count is missing, the endpoint probes the first store exactly once."""
        client, csrf = client_logged_in
        monkeypatch.setenv("API_KEY", "cloud-secret")
        datastores = [
            {"id": "ds-a"},  # no metrics at all
            {"id": "ds-b"},
        ]
        adapter = _make_adapter(
            datastores=datastores,
            inventory={"summary": {"snapshot_count": 3}, "groups": []},
        )
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "cloud", "type": "remote_backups",
                "api_base": "https://api.example.com",
                "api_key_env": "API_KEY",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        data = rv.get_json()
        assert data["ok"] is True
        steps = _get_steps(data)
        assert steps["datastores"]["status"] == "ok"
        # Fallback invoked exactly once on the first datastore with snapshot_cap=1
        adapter.fetch_backup_inventory.assert_called_once_with("ds-a", snapshot_cap=1)

    def test_remote_backups_inventory_probe_empty_falls_through_to_warn(self, monkeypatch, client_logged_in):
        client, csrf = client_logged_in
        monkeypatch.setenv("API_KEY", "cloud-secret")
        adapter = _make_adapter(
            datastores=[{"id": "ds-a"}],
            inventory={"summary": {"snapshot_count": 0}, "groups": []},
        )
        monkeypatch.setattr(webapp, "build_source", lambda e: adapter)
        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "cloud", "type": "remote_backups",
                "api_base": "https://api.example.com",
                "api_key_env": "API_KEY",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        steps = _get_steps(rv.get_json())
        assert steps["datastores"]["status"] == "warn"


# ── Form-vs-saved-config semantics ──────────────────────────────────────────

class TestFormDataSemantics:
    def test_endpoint_uses_body_entry_not_saved_config(self, monkeypatch, client_logged_in, tmp_path):
        """The test must probe whatever is in the body, even if a different id/url is on disk."""
        from tests.conftest import write_config
        write_config(tmp_path, {
            "sources": [{
                "id": "old-id", "type": "remote_backups",
                "api_base": "https://old.example.com", "api_key_env": "OLD_KEY",
            }],
        })
        client, csrf = client_logged_in
        monkeypatch.setenv("NEW_KEY", "secret")
        monkeypatch.delenv("OLD_KEY", raising=False)

        seen = {}
        def fake_build(entry):
            seen["id"] = entry["id"]
            seen["api_base"] = entry["api_base"]
            return _make_adapter(datastores=[{"id": "ds1", "metrics": {"backup_count": 2}}])

        monkeypatch.setattr(webapp, "build_source", fake_build)

        rv = client.post(
            "/api/alerting/test/source",
            json={"source": {
                "id": "new-id", "type": "remote_backups",
                "api_base": "https://new.example.com",
                "api_key_env": "NEW_KEY",
            }},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 200
        assert seen == {"id": "new-id", "api_base": "https://new.example.com"}
