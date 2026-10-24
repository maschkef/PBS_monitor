"""Multi-source WebUI route tests for Phase 8.

Covers:
- Cloud-only regression on /api/datastores and /api/datastores/metrics
- Cloud + PBS aggregation with correct source_type/source_id
- Error isolation when one source fails
- PBS backup browser shape
- /api/platform-stats with and without a remote_backups source
- /api/health aggregation and backward-compatible top-level shape
- Invalid config returns JSON 500

No real network calls are made; adapters are mocked via monkeypatch.
"""

import json
from pathlib import Path

import requests

import webui.app as webapp
from alerting.sources import HealthResult, PbsAdapter, RemoteBackupsAdapter


def _write_config(tmp_path: Path, data: dict) -> None:
    (tmp_path / "config.json").write_text(json.dumps(data))


def _cloud_datastore():
    return {
        "id": "ds-cloud-1",
        "name": "Cloud DS",
        "size_gb": 1000,
        "created_at": "2024-01-01T00:00:00Z",
        "metrics": {
            "used_bytes": 100,
            "available_bytes": 900,
            "used_percent": 10.0,
            "backup_count": 5,
        },
        "gc": {},
        "verification": {},
        "replication": {},
        "prune": {},
        "autoscaling": {},
        "immutable_backup": {},
    }


def _pbs_datastore():
    return {
        "id": "pbs:home:ssd",
        "name": "ssd",
        "source_type": "pbs",
        "source_id": "home",
        "size_gb": 2000,
        "metrics": {
            "used_bytes": 1000,
            "available_bytes": 1000,
            "used_percent": 50.0,
            "backup_count": 25,
        },
        "gc": {"status": "ok"},
        "verification": {"status": "ok"},
    }


def _visual_alerting_stub(*args, **kwargs):
    return {
        "health": "healthy",
        "alerts": [],
        "alert_count": 0,
        "max_priority": 0,
        "backup_status": {},
        "inventory_error": None,
        "state_source": "ephemeral",
        "rules_source": "ephemeral",
        "ignored_groups": [],
        "schedule_learning": {},
    }


def _patch_route_dependencies(monkeypatch):
    """Make datastore routes deterministic without involving the alert engine."""
    monkeypatch.setattr(webapp, "load_visual_group_rules", lambda: ({}, "ephemeral"))
    monkeypatch.setattr(webapp, "build_visual_alerting", _visual_alerting_stub)


class TestDatastoresRoute:
    def test_cloud_only_regression(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "x"})
        monkeypatch.setenv("API_KEY", "secret")
        _patch_route_dependencies(monkeypatch)
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_datastores", lambda self: [_cloud_datastore()])
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_rescale_log", lambda self, ds_id: [])

        rv = client_no_auth.get("/api/datastores")
        assert rv.status_code == 200
        data = rv.get_json()
        assert len(data) == 1
        assert data[0]["id"] == "ds-cloud-1"
        assert data[0]["source_type"] == "remote_backups"
        assert data[0]["source_id"] == "default"
        assert data[0]["metrics"]["used_percent"] == 10.0

    def test_cloud_and_pbs_aggregated(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "cloud", "type": "remote_backups", "api_base": "https://api.example.com"},
                {
                    "id": "home", "type": "pbs",
                    "base_url": "https://pbs.example.com:8007",
                    "node": "localhost",
                    "token_env": "PBS_TEST_TOKEN",
                },
            ],
        })
        monkeypatch.setenv("API_KEY", "secret")
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        _patch_route_dependencies(monkeypatch)
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_datastores", lambda self: [_cloud_datastore()])
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_rescale_log", lambda self, ds_id: [])
        monkeypatch.setattr(PbsAdapter, "fetch_datastores", lambda self: [_pbs_datastore()])

        rv = client_no_auth.get("/api/datastores")
        assert rv.status_code == 200
        data = rv.get_json()
        by_id = {d["id"]: d for d in data}
        assert set(by_id) == {"ds-cloud-1", "pbs:home:ssd"}
        assert by_id["ds-cloud-1"]["source_type"] == "remote_backups"
        assert by_id["ds-cloud-1"]["source_id"] == "cloud"
        assert by_id["pbs:home:ssd"]["source_type"] == "pbs"
        assert by_id["pbs:home:ssd"]["source_id"] == "home"
        assert "X-Source-Errors" not in rv.headers

    def test_one_source_failing_others_still_return(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "cloud", "type": "remote_backups", "api_base": "https://api.example.com"},
                {
                    "id": "home", "type": "pbs",
                    "base_url": "https://pbs.example.com:8007",
                    "node": "localhost",
                    "token_env": "PBS_TEST_TOKEN",
                },
            ],
        })
        monkeypatch.setenv("API_KEY", "secret")
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        _patch_route_dependencies(monkeypatch)
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_datastores", lambda self: [_cloud_datastore()])
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_rescale_log", lambda self, ds_id: [])
        monkeypatch.setattr(
            PbsAdapter, "fetch_datastores",
            lambda self: (_ for _ in ()).throw(requests.ConnectionError("pbs offline")),
        )

        rv = client_no_auth.get("/api/datastores")
        assert rv.status_code == 200
        data = rv.get_json()
        assert len(data) == 1
        assert data[0]["id"] == "ds-cloud-1"
        assert json.loads(rv.headers["X-Source-Errors"]) == {"home": "pbs offline"}

    def test_all_sources_failing_returns_502(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "x"})
        monkeypatch.setenv("API_KEY", "secret")
        _patch_route_dependencies(monkeypatch)
        monkeypatch.setattr(
            RemoteBackupsAdapter, "fetch_datastores",
            lambda self: (_ for _ in ()).throw(requests.ConnectionError("cloud offline")),
        )

        rv = client_no_auth.get("/api/datastores")
        assert rv.status_code == 502

    def test_pbs_features_section_hidden_in_dashboard_js(self):
        dashboard = (
            Path(__file__).parent.parent / "webui/static/js/dashboard.js"
        ).read_text()
        features_start = dashboard.index("<!-- Features (cloud-only) -->")
        snippet = dashboard[features_start:features_start + 400]
        assert "ds.source_type !== 'pbs'" in snippet


class TestDatastoresMetricsRoute:
    def test_metrics_aggregates_both_sources(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "cloud", "type": "remote_backups", "api_base": "https://api.example.com"},
                {
                    "id": "home", "type": "pbs",
                    "base_url": "https://pbs.example.com:8007",
                    "node": "localhost",
                    "token_env": "PBS_TEST_TOKEN",
                },
            ],
        })
        monkeypatch.setenv("API_KEY", "secret")
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        monkeypatch.setattr(webapp, "load_visual_group_rules", lambda: ({}, "ephemeral"))
        monkeypatch.setattr(webapp, "build_visual_alerting", _visual_alerting_stub)
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_datastores", lambda self: [_cloud_datastore()])
        monkeypatch.setattr(PbsAdapter, "fetch_datastores", lambda self: [_pbs_datastore()])

        rv = client_no_auth.get("/api/datastores/metrics")
        assert rv.status_code == 200
        data = rv.get_json()
        by_id = {d["id"]: d for d in data}
        assert set(by_id) == {"ds-cloud-1", "pbs:home:ssd"}
        assert by_id["pbs:home:ssd"]["source_type"] == "pbs"
        assert "X-Source-Errors" not in rv.headers

    def test_metrics_marks_partial_source_failure(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "cloud", "type": "remote_backups", "api_base": "https://api.example.com"},
                {
                    "id": "home", "type": "pbs",
                    "base_url": "https://pbs.example.com:8007",
                    "node": "localhost",
                    "token_env": "PBS_TEST_TOKEN",
                },
            ],
        })
        monkeypatch.setenv("API_KEY", "secret")
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        _patch_route_dependencies(monkeypatch)
        monkeypatch.setattr(RemoteBackupsAdapter, "fetch_datastores", lambda self: [_cloud_datastore()])
        monkeypatch.setattr(
            PbsAdapter, "fetch_datastores",
            lambda self: (_ for _ in ()).throw(requests.ConnectionError("pbs offline")),
        )

        rv = client_no_auth.get("/api/datastores/metrics")
        assert rv.status_code == 200
        assert [item["id"] for item in rv.get_json()] == ["ds-cloud-1"]
        assert json.loads(rv.headers["X-Source-Errors"]) == {"home": "pbs offline"}


class TestDatastoreBackupsRoute:
    def test_pbs_backup_browser_shape(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [{
                "id": "home", "type": "pbs",
                "base_url": "https://pbs.example.com:8007",
                "node": "localhost",
                "token_env": "PBS_TEST_TOKEN",
            }],
        })
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")

        calls = []

        def fake_fetch_backup_inventory(self, ds_id, *, snapshot_cap):
            calls.append((ds_id, snapshot_cap))
            return {
                "summary": {"namespace_count": 1, "group_count": 1, "snapshot_count": 1},
                "groups": [{
                    "namespace": "",
                    "backup_type": "ct",
                    "backup_id": "110",
                    "display_name": "home-ct",
                    "comment": "home-ct",
                    "last_backup_at": "2024-01-01T00:00:00+00:00",
                    "backup_count": 1,
                    "current_snapshots": [{
                        "backup_time": 1704067200,
                        "size": 4096,
                        "protected": True,
                        "comment": "snap1",
                    }],
                }],
            }

        monkeypatch.setattr(PbsAdapter, "fetch_backup_inventory", fake_fetch_backup_inventory)

        rv = client_no_auth.get("/api/datastores/pbs:home:ssd/backups")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["datastore_id"] == "pbs:home:ssd"
        assert data["protocols"] == {}
        assert data["summary"] == {"namespace_count": 1, "group_count": 1, "snapshot_count": 1}
        assert len(data["namespaces"]) == 1
        ns = data["namespaces"][0]
        assert ns["ns"] == ""
        assert ns["label"] == "root"
        assert ns["group_count"] == 1
        assert ns["snapshot_count"] == 1
        [grp] = ns["groups"]
        assert grp["backup_type"] == "ct"
        assert grp["backup_id"] == "110"
        assert grp["snapshot_count"] == 1
        assert grp["last_backup_iso"] is not None
        assert "snapshots" in grp
        [snap] = grp["snapshots"]
        assert snap["backup_time"] == 1704067200
        assert snap["size_human"] is not None
        assert snap["file_count"] == 0
        assert calls == [("pbs:home:ssd", 50)]  # default snapshot_retention_count after auto-migration

    def test_unknown_datastore_prefix_returns_404(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "x"})
        monkeypatch.setenv("API_KEY", "secret")
        rv = client_no_auth.get("/api/datastores/pbs:home:ssd/backups")
        assert rv.status_code == 404


class TestPlatformStatsRoute:
    def test_nulls_without_remote_backups_source(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [{
                "id": "home", "type": "pbs",
                "base_url": "https://pbs.example.com:8007",
                "node": "localhost",
                "token_env": "PBS_TEST_TOKEN",
            }],
        })
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        rv = client_no_auth.get("/api/platform-stats")
        assert rv.status_code == 200
        assert rv.get_json() == {"storage": None, "backups_30d": None, "traffic_30d": None}

    def test_delegates_to_remote_backups_adapter(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {"api_base": "https://api.example.com", "ntfy_topic": "x"})
        monkeypatch.setenv("API_KEY", "secret")

        calls = []

        def fake_get_public(self, path):
            calls.append(path)
            return {"value": path}

        monkeypatch.setattr(RemoteBackupsAdapter, "get_public", fake_get_public)

        rv = client_no_auth.get("/api/platform-stats")
        assert rv.status_code == 200
        data = rv.get_json()
        assert "/public/total-storage" in calls
        assert data["storage"] == {"value": "/public/total-storage"}


class TestHealthRoute:
    def test_aggregates_cloud_and_pbs(self, client_no_auth, monkeypatch, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "cloud", "type": "remote_backups", "api_base": "https://api.example.com"},
                {
                    "id": "home", "type": "pbs",
                    "base_url": "https://pbs.example.com:8007",
                    "node": "localhost",
                    "token_env": "PBS_TEST_TOKEN",
                },
            ],
        })
        monkeypatch.setenv("API_KEY", "secret")
        monkeypatch.setenv("PBS_TEST_TOKEN", "tok")
        monkeypatch.setattr(
            RemoteBackupsAdapter, "check_health",
            lambda self: HealthResult(status="ok", message="cloud ok"),
        )
        monkeypatch.setattr(
            PbsAdapter, "check_health",
            lambda self: HealthResult(status="down", message="pbs offline"),
        )

        rv = client_no_auth.get("/api/health")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["status"] == "ok"  # first remote_backups source
        assert data["message"] == "cloud ok"
        assert data["overall"] == "down"
        assert len(data["sources"]) == 2
        by_id = {s["source_id"]: s for s in data["sources"]}
        assert by_id["cloud"]["status"] == "ok"
        assert by_id["home"]["status"] == "down"


class TestInvalidConfig:
    def test_invalid_config_returns_json_500(self, client_no_auth, tmp_path):
        _write_config(tmp_path, {
            "sources": [
                {"id": "a", "type": "remote_backups", "api_base": "https://x"},
                {"id": "a", "type": "remote_backups", "api_base": "https://y"},
            ],
        })
        rv = client_no_auth.get("/api/datastores")
        assert rv.status_code == 500
        assert "error" in rv.get_json()
