"""Tests for the Phase 4 PbsAdapter.

Mock-based: no real network calls against pbs.* hosts. Verifies the HTTP
contract against the PBS API (headers, URLs, response normalisation) plus
error-path handling (HTTP 401/403, SSL fingerprint mismatch, missing env
var, per-store failures).
"""

from __future__ import annotations

import pytest
import requests

from alerting.sources import (
    HealthResult,
    PbsAdapter,
    SourceConnectionError,
)
from alerting.sources.pbs import _FingerprintAdapter


class _FakeResp:
    """Stand-in for requests.Response. `data_payload` is wrapped in the
    PBS `{"data": ...}` envelope by `.json()`."""

    def __init__(
        self,
        data_payload=None,
        *,
        status_code: int = 200,
        raise_for_status_exc: Exception | None = None,
    ):
        self._data_payload = data_payload
        self.status_code = status_code
        self._exc = raise_for_status_exc

    def raise_for_status(self):
        if self._exc is not None:
            raise self._exc

    def json(self):
        return {"data": self._data_payload}


class _FakeSession:
    """Collects .get() calls and dispatches to canned responses matched by
    URL suffix. Response may be a _FakeResp, or an Exception to raise."""

    def __init__(self, responses: dict):
        self._responses = responses
        self.calls: list[dict] = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append({
            "url": url,
            "headers": headers or {},
            "params": params,
            "timeout": timeout,
        })
        for suffix, resp in self._responses.items():
            if url.endswith(suffix):
                if isinstance(resp, Exception):
                    raise resp
                return resp
        raise AssertionError(f"unexpected URL in test: {url}")


def _make_adapter(monkeypatch, token="tok", **kwargs) -> PbsAdapter:
    monkeypatch.setenv("PBS_TOKEN", token)
    defaults = {
        "source_id": "home-lab",
        "base_url": "https://pbs.example.com:8007",
        "node": "localhost",
        "token_env": "PBS_TOKEN",
    }
    defaults.update(kwargs)
    return PbsAdapter(**defaults)


class TestCheckHealth:
    def test_ok(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._session_cache = _FakeSession({
            "/api2/json/version": _FakeResp({"version": "4.2", "release": "0"}),
        })
        result = adapter.check_health()
        assert isinstance(result, HealthResult)
        assert result.status == "ok"
        assert "4.2" in (result.message or "")

    def test_401_returns_degraded(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        resp = requests.Response()
        resp.status_code = 401
        adapter._session_cache = _FakeSession({
            "/api2/json/version": _FakeResp(
                raise_for_status_exc=requests.HTTPError(response=resp),
            ),
        })
        result = adapter.check_health()
        assert result.status == "degraded"
        assert "401" in (result.message or "")

    def test_403_returns_degraded(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        resp = requests.Response()
        resp.status_code = 403
        adapter._session_cache = _FakeSession({
            "/api2/json/version": _FakeResp(
                raise_for_status_exc=requests.HTTPError(response=resp),
            ),
        })
        result = adapter.check_health()
        assert result.status == "degraded"

    def test_500_returns_down(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        resp = requests.Response()
        resp.status_code = 500
        adapter._session_cache = _FakeSession({
            "/api2/json/version": _FakeResp(
                raise_for_status_exc=requests.HTTPError(response=resp),
            ),
        })
        result = adapter.check_health()
        assert result.status == "down"

    def test_connection_error_returns_down(self, monkeypatch):
        # Use .invalid TLD so diagnose_network's DNS probe fails fast and
        # we don't block on a real TCP connect.
        adapter = _make_adapter(
            monkeypatch, base_url="https://pbs.example.invalid:8007",
        )
        adapter._session_cache = _FakeSession({
            "/api2/json/version": requests.ConnectionError("boom"),
        })
        result = adapter.check_health()
        assert result.status == "down"
        assert result.error is not None

    def test_missing_token_env_returns_degraded(self, monkeypatch):
        monkeypatch.delenv("MISSING_TOKEN", raising=False)
        adapter = PbsAdapter(
            source_id="x", base_url="https://pbs.example.com:8007",
            node="localhost", token_env="MISSING_TOKEN",
        )
        result = adapter.check_health()
        assert result.status == "degraded"
        assert "MISSING_TOKEN" in (result.message or "")


class TestFetchDatastores:
    def test_auto_discovery(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._session_cache = _FakeSession({
            "/api2/json/admin/datastore": _FakeResp([
                {"store": "ssd", "backend-type": "filesystem"},
                {"store": "cold", "backend-type": "filesystem"},
            ]),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 200, "used": 100, "avail": 100,
            }),
            "/admin/datastore/cold/status": _FakeResp({
                "total": 1000, "used": 250, "avail": 750,
            }),
        })
        result = adapter.fetch_datastores()
        assert len(result) == 2
        assert {r["id"] for r in result} == {
            "pbs:home-lab:ssd", "pbs:home-lab:cold",
        }

    def test_explicit_list_skips_discovery(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        session = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 200, "used": 100, "avail": 100,
            }),
        })
        adapter._session_cache = session
        adapter.fetch_datastores()
        # Discovery endpoint must NOT be called.
        assert not any(
            call["url"].endswith("/admin/datastore")
            for call in session.calls
        )

    def test_metrics_normalisation(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 200, "used": 100, "avail": 100,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["metrics"] == {
            "used_bytes": 100,
            "available_bytes": 100,
            "used_percent": 50.0,
            "backup_count": 0,
        }

    def test_id_format(self, monkeypatch):
        adapter = _make_adapter(
            monkeypatch, source_id="home-lab", datastores=["backup01"],
        )
        adapter._session_cache = _FakeSession({
            "/admin/datastore/backup01/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["id"] == "pbs:home-lab:backup01"

    def test_omits_optional_fields(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        for field in (
            "gc", "verification", "prune",
            "autoscaling", "rescale_history",
            "immutable_backup", "replication",
        ):
            assert field not in ds, (
                f"{field!r} must be absent, not present with None/{{}}"
            )

    def test_label_overrides_name(self, monkeypatch):
        adapter = _make_adapter(
            monkeypatch, label="Home PBS", datastores=["ssd"],
        )
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["name"] == "Home PBS"

    def test_name_defaults_to_store(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["name"] == "ssd"

    def test_source_type_and_id_set(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["source_type"] == "pbs"
        assert ds["source_id"] == "home-lab"

    def test_per_store_failure_is_logged_and_skipped(
        self, monkeypatch, capsys,
    ):
        adapter = _make_adapter(monkeypatch, datastores=["ssd", "broken"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
            "/admin/datastore/broken/status":
                requests.ConnectionError("broken offline"),
        })
        result = adapter.fetch_datastores()
        assert [r["id"] for r in result] == ["pbs:home-lab:ssd"]
        captured = capsys.readouterr()
        assert "broken" in captured.out

    def test_size_gb_derived_from_total(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 1024**3 * 10, "used": 1024**3 * 5,
                "avail": 1024**3 * 5,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["size_gb"] == 10.0


class TestApiGet:
    def test_sends_pbs_api_token_header(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, token="root@pam!mon:abc")
        session = _FakeSession({
            "/api2/json/version": _FakeResp({"version": "4.2"}),
        })
        adapter._session_cache = session
        adapter.check_health()
        assert session.calls, "no HTTP calls recorded"
        assert (
            session.calls[0]["headers"]["Authorization"]
            == "PBSAPIToken=root@pam!mon:abc"
        )

    def test_passes_15s_timeout(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        session = _FakeSession({
            "/api2/json/version": _FakeResp({"version": "4.2"}),
        })
        adapter._session_cache = session
        adapter.check_health()
        assert session.calls[0]["timeout"] == 15

    def test_missing_token_env_raises_runtime_error(self, monkeypatch):
        monkeypatch.delenv("UNSET_TOKEN", raising=False)
        adapter = PbsAdapter(
            source_id="x", base_url="https://pbs.example.com:8007",
            node="localhost", token_env="UNSET_TOKEN",
        )
        with pytest.raises(RuntimeError, match="UNSET_TOKEN not set"):
            adapter._api_get("/api2/json/version")

    def test_ssl_error_wrapped_as_source_connection_error(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        adapter._session_cache = _FakeSession({
            "/api2/json/version": requests.exceptions.SSLError("mismatch"),
        })
        with pytest.raises(SourceConnectionError, match="TLS verification"):
            adapter._api_get("/api2/json/version")


class TestFetchBackupInventory:
    def test_root_only_namespace_single_group(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([{"ns": ""}]),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "ct",
                "backup-id": 110,
                "last-backup": 1700000000,
                "backup-count": 1,
                "comment": "home-ct",
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp([{
                "backup-type": "ct",
                "backup-id": 110,
                "backup-time": 1700000000,
                "size": 4096,
                "protected": True,
                "comment": "snap1",
            }]),
        })
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=50,
        )
        assert inv["summary"] == {
            "namespace_count": 1,
            "group_count": 1,
            "snapshot_count": 1,
        }
        [grp] = inv["groups"]
        assert grp["group_key"] == '["","ct","110"]'
        assert grp["namespace"] == ""
        assert grp["backup_type"] == "ct"
        assert grp["backup_id"] == "110"
        assert grp["backup_count"] == 1
        assert grp["protected_snapshot_count"] == 1
        assert grp["display_name"] == "snap1"
        assert grp["last_backup_at"] is not None
        [snap] = grp["current_snapshots"]
        assert snap["backup_time"] == 1700000000
        assert snap["size"] == 4096
        assert snap["protected"] is True
        assert snap["comment"] == "snap1"

    def test_multiple_namespaces_sorted_root_first(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        session = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp(
                [{"ns": "prod"}, {"ns": ""}],
            ),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 100,
                "last-backup": 1700000000,
                "backup-count": 1,
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 100,
                "backup-time": 1700000000,
                "size": 1024,
                "protected": False,
            }]),
        })
        adapter._session_cache = session
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=50,
        )
        assert inv["summary"]["namespace_count"] == 2
        # Record the order of ns-params on groups fetches.
        ns_params = [
            (c.get("params") or {}).get("ns")
            for c in session.calls
            if c["url"].endswith("/admin/datastore/ssd/groups")
        ]
        # Root (None, no param) must come first, then "prod".
        assert ns_params == [None, "prod"]

    def test_empty_namespace_response_falls_back_to_root(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        session = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([]),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "host",
                "backup-id": "pve",
                "last-backup": 1700000000,
                "backup-count": 1,
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp([{
                "backup-type": "host",
                "backup-id": "pve",
                "backup-time": 1700000000,
                "size": 2048,
                "protected": False,
            }]),
        })
        adapter._session_cache = session
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=50,
        )
        assert inv["summary"]["namespace_count"] == 1
        assert inv["summary"]["group_count"] == 1
        # Root namespace fetched with no ns-param (= PBS default).
        group_calls = [
            c for c in session.calls
            if c["url"].endswith("/admin/datastore/ssd/groups")
        ]
        assert group_calls[0]["params"] is None

    def test_snapshot_cap_limits_current_snapshots(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        snapshots = [
            {
                "backup-type": "vm",
                "backup-id": 200,
                "backup-time": 1700000000 + i * 3600,
                "size": 1024,
                "protected": False,
            }
            for i in range(5)
        ]
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([{"ns": ""}]),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 200,
                "last-backup": 1700000000 + 4 * 3600,
                "backup-count": 5,
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp(snapshots),
        })
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=2,
        )
        [grp] = inv["groups"]
        assert grp["backup_count"] == 5
        assert len(grp["current_snapshots"]) == 2
        # Newest two snapshots retained (sorted desc by backup_time).
        assert [s["backup_time"] for s in grp["current_snapshots"]] == [
            1700000000 + 4 * 3600,
            1700000000 + 3 * 3600,
        ]

    def test_protected_snapshot_count_aggregates(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([{"ns": ""}]),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 300,
                "last-backup": 1700000000,
                "backup-count": 3,
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp([
                {
                    "backup-type": "vm", "backup-id": 300,
                    "backup-time": 1700000000, "size": 1024,
                    "protected": True,
                },
                {
                    "backup-type": "vm", "backup-id": 300,
                    "backup-time": 1700001000, "size": 1024,
                    "protected": True,
                },
                {
                    "backup-type": "vm", "backup-id": 300,
                    "backup-time": 1700002000, "size": 1024,
                    "protected": False,
                },
            ]),
        })
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=50,
        )
        [grp] = inv["groups"]
        assert grp["protected_snapshot_count"] == 2
        assert grp["backup_count"] == 3

    def test_malformed_datastore_id_raises_value_error(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        with pytest.raises(ValueError, match="pbs:<source>:<store>"):
            adapter.fetch_backup_inventory(
                "wrong:format", snapshot_cap=50,
            )

    def test_http_error_on_groups_propagates(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        http_err = requests.HTTPError("500 Internal Server Error")
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([{"ns": ""}]),
            "/admin/datastore/ssd/groups": _FakeResp(
                raise_for_status_exc=http_err,
            ),
        })
        with pytest.raises(requests.HTTPError):
            adapter.fetch_backup_inventory(
                "pbs:home-lab:ssd", snapshot_cap=50,
            )


class TestFetchRescaleLog:
    def test_returns_empty_list(self, monkeypatch):
        adapter = _make_adapter(monkeypatch)
        assert adapter.fetch_rescale_log("pbs:home-lab:ssd") == []


class TestFingerprintAdapter:
    def test_accepts_colon_separated_sha256(self):
        fp = (
            "EC:D4:1B:18:7C:DA:D8:15:EB:8A:23:B8:E7:55:BB:DE:"
            "F4:EC:8D:57:0C:7D:A3:40:CD:B4:34:A9:CB:3F:18:59"
        )
        adapter = _FingerprintAdapter(fp)
        assert len(adapter._fingerprint) == 64
        assert adapter._fingerprint == adapter._fingerprint.lower()

    def test_accepts_unseparated_sha256(self):
        fp = "a" * 64
        adapter = _FingerprintAdapter(fp)
        assert adapter._fingerprint == "a" * 64

    def test_rejects_wrong_length(self):
        with pytest.raises(ValueError, match="64-character"):
            _FingerprintAdapter("deadbeef")

    def test_rejects_non_hex(self):
        with pytest.raises(ValueError, match="64-character"):
            _FingerprintAdapter("g" * 64)
