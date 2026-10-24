"""Tests for WebUI source configuration (Phase 8.1)."""

import pytest

import webui.app as webapp
from alerting import monitor as alert_monitor
from tests.conftest import TEST_PASSWORD, do_login, read_config, write_config


class TestSourceConfigGet:
    def test_get_includes_source_env_status_and_no_secret_values(
        self, monkeypatch, client_no_auth, tmp_path,
    ):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        monkeypatch.setenv("TEST_CLOUD_TOKEN", "cloud-secret-value")
        monkeypatch.setenv("PBS_HOMELAB_TOKEN", "pbs-secret-value")
        write_config(tmp_path, {
            "sources": [
                {
                    "id": "default",
                    "type": "remote_backups",
                    "api_base": "https://api.remote-backups.com",
                    "api_key_env": "TEST_CLOUD_TOKEN",
                },
                {
                    "id": "home-lab",
                    "type": "pbs",
                    "base_url": "https://pbs.home.lan:8007",
                    "node": "localhost",
                    "token_env": "PBS_HOMELAB_TOKEN",
                },
            ],
        })

        rv = client_no_auth.get("/api/alerting/config")
        assert rv.status_code == 200
        body = rv.get_json()
        status = body["source_env_status"]
        assert status["default"]["env_name"] == "TEST_CLOUD_TOKEN"
        assert status["default"]["is_set"] is True
        assert status["home-lab"]["env_name"] == "PBS_HOMELAB_TOKEN"
        assert status["home-lab"]["is_set"] is True

        raw = rv.data.decode("utf-8")
        assert "cloud-secret-value" not in raw
        assert "pbs-secret-value" not in raw


class TestSourceConfigSave:
    def _post(self, client, csrf, payload, tmp_path, monkeypatch):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        return client.post(
            "/api/alerting/config",
            json=payload,
            headers={"X-CSRF-Token": csrf},
        )

    def test_roundtrip_cloud_and_multiple_pbs_sources(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"ntfy_topic": "legacy-topic"})
        csrf = do_login(client_auth, TEST_PASSWORD)

        payload_sources = [
            {
                "id": "cloud-1",
                "type": "remote_backups",
                "api_base": "https://api.example.com",
                "api_key_env": "CLOUD1_TOKEN",
            },
            {
                "id": "home-lab",
                "type": "pbs",
                "label": "Home PBS",
                "base_url": "https://pbs.home.lan:8007",
                "node": "localhost",
                "token_env": "PBS_HOMELAB_TOKEN",
                "verify_tls": True,
                "datastores": ["ssd"],
            },
            {
                "id": "office",
                "type": "pbs",
                "base_url": "https://pbs.office.lan:8007",
                "node": "pbs-01",
                "token_env": "PBS_OFFICE_TOKEN",
            },
        ]
        rv = self._post(client_auth, csrf, {"ntfy_topic": "new-topic", "sources": payload_sources}, tmp_path, monkeypatch)
        assert rv.status_code == 200

        saved = read_config(tmp_path)
        assert saved["ntfy_topic"] == "new-topic"
        assert saved["sources"][0]["id"] == "cloud-1"
        assert saved["sources"][0]["api_key_env"] == "CLOUD1_TOKEN"
        assert saved["sources"][1]["label"] == "Home PBS"
        assert saved["sources"][1]["datastores"] == ["ssd"]
        assert saved["sources"][2]["id"] == "office"
        assert saved["sources"][2]["verify_tls"] is True
        assert saved["sources"][2].get("label") is None

        rv = client_auth.get("/api/alerting/config")
        body = rv.get_json()
        assert body["config"]["sources"] == saved["sources"]
        assert body["source_env_status"]["cloud-1"]["is_set"] is False
        assert body["source_env_status"]["home-lab"]["env_name"] == "PBS_HOMELAB_TOKEN"

        config_path = tmp_path / "config.json"
        monkeypatch.setattr(alert_monitor, "CONFIG_PATH", config_path)
        loaded = alert_monitor.load_config()
        assert [s["id"] for s in loaded["sources"]] == ["cloud-1", "home-lab", "office"]

    def test_edit_and_delete_source(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {
            "sources": [
                {"id": "keep", "type": "pbs", "base_url": "https://a", "node": "n", "token_env": "T"},
                {"id": "remove", "type": "pbs", "base_url": "https://b", "node": "n", "token_env": "T"},
            ],
        })
        csrf = do_login(client_auth, TEST_PASSWORD)

        rv = self._post(client_auth, csrf, {"sources": [
            {
                "id": "keep",
                "type": "pbs",
                "base_url": "https://new-a",
                "node": "n2",
                "token_env": "T2",
                "label": "Kept",
            },
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 200

        saved = read_config(tmp_path)
        assert [s["id"] for s in saved["sources"]] == ["keep"]
        assert saved["sources"][0]["base_url"] == "https://new-a"
        assert saved["sources"][0]["node"] == "n2"
        assert saved["sources"][0]["token_env"] == "T2"
        assert saved["sources"][0]["label"] == "Kept"


    def test_empty_sources_becomes_synthetic_default(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"api_base": "https://api.example.com"})
        csrf = do_login(client_auth, TEST_PASSWORD)

        rv = self._post(client_auth, csrf, {"sources": []}, tmp_path, monkeypatch)
        assert rv.status_code == 200

        saved = read_config(tmp_path)
        assert saved["sources"] == [{
            "id": "default",
            "type": "remote_backups",
            "api_base": "https://api.example.com",
            "api_key_env": "API_KEY",
        }]

    def test_legacy_sources_preserved_when_not_in_payload(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "old"})
        csrf = do_login(client_auth, TEST_PASSWORD)

        rv = self._post(client_auth, csrf, {"ntfy_topic": "new"}, tmp_path, monkeypatch)
        assert rv.status_code == 200

        saved = read_config(tmp_path)
        assert "sources" not in saved
        assert saved["ntfy_topic"] == "new"


class TestSourceConfigValidation:
    def _post(self, client, csrf, payload, tmp_path, monkeypatch):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        return client.post(
            "/api/alerting/config",
            json=payload,
            headers={"X-CSRF-Token": csrf},
        )

    @pytest.fixture()
    def setup(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        write_config(tmp_path, {"ntfy_topic": "preserved", "sources": [
            {"id": "existing", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T"},
        ]})
        return do_login(client_auth, TEST_PASSWORD)

    @pytest.mark.parametrize("bad_id", ["BadID", "-bad", "has space", ""])
    def test_invalid_id_rejected(self, monkeypatch, client_auth, tmp_path, setup, bad_id):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": bad_id, "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        saved = read_config(tmp_path)
        assert saved["sources"][0]["id"] == "existing"
        assert saved["ntfy_topic"] == "preserved"

    def test_duplicate_id_rejected(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T"},
            {"id": "a", "type": "pbs", "base_url": "https://y", "node": "n", "token_env": "T"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "duplicates" in rv.get_json()["error"].lower()

    def test_unknown_type_rejected(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "weird", "api_base": "https://x"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400

    @pytest.mark.parametrize("missing", ["base_url", "token_env"])
    def test_pbs_missing_required_field_rejected(self, monkeypatch, client_auth, tmp_path, setup, missing):
        csrf = setup
        entry = {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T"}
        entry.pop(missing)
        rv = self._post(client_auth, csrf, {"sources": [entry]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert missing in rv.get_json()["error"]

    def test_pbs_missing_node_defaults_to_localhost(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "token_env": "T"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert saved["sources"][0]["node"] == "localhost"

    def test_cloud_missing_api_base_rejected(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "remote_backups"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "api_base" in rv.get_json()["error"]

    @pytest.mark.parametrize("bad_url", [
        "not-a-url",
        "ftp://example.com",
        "https://",
        "https://user:pass@example.com",
    ])
    def test_invalid_url_rejected(self, monkeypatch, client_auth, tmp_path, setup, bad_url):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "remote_backups", "api_base": bad_url},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400

    def test_verify_tls_false_without_fingerprint_rejected(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T", "verify_tls": False},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "fingerprint" in rv.get_json()["error"].lower()

    def test_whitespace_only_env_name_rejected(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "   "},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "token_env" in rv.get_json()["error"]

    @pytest.mark.parametrize("bad_node", [
        "pbs.fmaschke.de",
        "https://pbs.example.com",
        "pbs/node",
    ])
    def test_invalid_node_name_rejected(self, monkeypatch, client_auth, tmp_path, setup, bad_node):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": bad_node, "token_env": "T"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "short hostname" in rv.get_json()["error"]

    @pytest.mark.parametrize("bad_env", ["PBS-TOKEN", "1PBS_TOKEN", "PBS TOKEN"])
    def test_invalid_token_env_name_rejected(self, monkeypatch, client_auth, tmp_path, setup, bad_env):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": bad_env},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "token_env" in rv.get_json()["error"]

    @pytest.mark.parametrize("bad_env", ["API-KEY", "1API_KEY", "API KEY"])
    def test_invalid_api_key_env_name_rejected(self, monkeypatch, client_auth, tmp_path, setup, bad_env):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "remote_backups", "api_base": "https://x", "api_key_env": bad_env},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 400
        assert "api_key_env" in rv.get_json()["error"]

    def test_unknown_source_keys_are_not_persisted(self, monkeypatch, client_auth, tmp_path, setup):
        csrf = setup
        rv = self._post(client_auth, csrf, {"sources": [
            {"id": "a", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T", "secret_token": "shhh"},
        ]}, tmp_path, monkeypatch)
        assert rv.status_code == 200
        saved = read_config(tmp_path)
        assert "secret_token" not in saved["sources"][0]
        assert "shhh" not in (tmp_path / "config.json").read_text()


class TestSourceConfigReadOnly:
    def test_read_only_rejects_source_changes(self, monkeypatch, client_auth, tmp_path):
        monkeypatch.setattr(webapp, "ALERTING_CONFIG_PATH", tmp_path / "config.json")
        monkeypatch.setattr(webapp, "WEBUI_READ_ONLY", True)
        write_config(tmp_path, {"sources": [
            {"id": "existing", "type": "pbs", "base_url": "https://x", "node": "n", "token_env": "T"},
        ]})
        csrf = do_login(client_auth, TEST_PASSWORD)
        rv = client_auth.post(
            "/api/alerting/config",
            json={"sources": [{"id": "new", "type": "pbs", "base_url": "https://y", "node": "n", "token_env": "T"}]},
            headers={"X-CSRF-Token": csrf},
        )
        assert rv.status_code == 403
        saved = read_config(tmp_path)
        assert saved["sources"][0]["id"] == "existing"