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


class _FakeSession(requests.Session):
    """Collects .get() calls and dispatches to canned responses matched by
    URL suffix. Response may be a _FakeResp, or an Exception to raise.

    For endpoints that share a URL but differ only in query params (notably
    `/nodes/<node>/tasks` with varying `typefilter`), pass a dict value with
    a `"params"` sub-dict: the entry matches when every listed param equals
    the request's. The special key `"_default"` is used as a fallback.
    """

    def __init__(self, responses: dict):
        # Intentionally skip Session.__init__ — tests only need .get(), and the
        # real init builds HTTPAdapters / cookie jars we don't want here.
        self._responses = responses
        self.calls: list[dict] = []

    def get(self, url, **kwargs):  # type: ignore[override]
        headers = kwargs.get("headers")
        params = kwargs.get("params")
        timeout = kwargs.get("timeout")
        self.calls.append({
            "url": url,
            "headers": headers or {},
            "params": params,
            "timeout": timeout,
        })
        for suffix, resp in self._responses.items():
            if not url.endswith(suffix):
                continue
            if isinstance(resp, dict) and "_variants" in resp:
                variants = resp["_variants"]
                for variant in variants:
                    match_params = variant.get("params") or {}
                    request_params = params or {}
                    if all(
                        request_params.get(k) == v
                        for k, v in match_params.items()
                    ):
                        inner = variant["response"]
                        if isinstance(inner, Exception):
                            raise inner
                        return inner
                raise AssertionError(
                    f"no variant matched params {params!r} for {url}"
                )
            if isinstance(resp, Exception):
                raise resp
            return resp
        raise AssertionError(f"unexpected URL in test: {url}")


def _make_adapter(monkeypatch, token="tok", **kwargs) -> PbsAdapter:
    monkeypatch.setenv("PBS_TOKEN", token)
    return PbsAdapter(
        source_id=kwargs.pop("source_id", "home-lab"),
        base_url=kwargs.pop("base_url", "https://pbs.example.com:8007"),
        node=kwargs.pop("node", "localhost"),
        token_env=kwargs.pop("token_env", "PBS_TOKEN"),
        **kwargs,
    )


def _phase6_empty_defaults(*stores: str) -> dict:
    """Empty-response mocks for the GC/Verify/Prune endpoints introduced in
    Phase 6, plus the namespace/groups endpoints `_count_backups` needs.
    Lets `TestFetchDatastores` cases focus on their own invariants while
    still letting `_normalise_status` complete without AssertionError.
    """
    defaults = {
        "/tasks": _FakeResp([]),  # matches /nodes/<node>/tasks suffix
        "/config/verify": _FakeResp([]),
        "/config/prune": _FakeResp([]),
    }
    for store in stores or ("ssd",):
        defaults[f"/config/datastore/{store}"] = _FakeResp({})
        defaults.update(_backup_count_mocks(store))
    return defaults


def _backup_count_mocks(store: str) -> dict:
    """Empty namespace/groups responses so `_count_backups` returns 0 without
    AssertionError in tests that focus on an unrelated code path. Uses the
    full `/admin/datastore/<store>/…` suffix so tests spreading this can
    override with the same key — a shorter suffix would still match the
    overriding URL and win via insertion order."""
    return {
        f"/admin/datastore/{store}/namespace": _FakeResp([]),
        f"/admin/datastore/{store}/groups": _FakeResp([]),
    }


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
            **_phase6_empty_defaults("ssd", "cold"),
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
            **_phase6_empty_defaults("ssd"),
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
            **_phase6_empty_defaults("ssd"),
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
            **_phase6_empty_defaults("backup01"),
            "/admin/datastore/backup01/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["id"] == "pbs:home-lab:backup01"

    def test_omits_optional_fields_when_pbs_has_no_data(self, monkeypatch):
        """When PBS returns no tasks and no verify/prune jobs for the store,
        the adapter must not populate gc/verification/prune at all — the
        monitor's `if gc:` / `if verification:` guards rely on absence to
        decide whether to run the alert path. autoscaling/rescale_history/
        immutable_backup/replication are PBS-foreign and always absent."""
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        for field in (
            "autoscaling", "rescale_history",
            "immutable_backup", "replication",
        ):
            assert field not in ds, (
                f"{field!r} must be absent, not present with None/{{}}"
            )
        # Phase 6: gc/verification are present but reflect "never ran" when
        # PBS returned no tasks; prune stays absent because /config/prune is
        # empty for this store.
        assert ds["gc"]["status"] == "never"
        assert ds["gc"]["last_run"] is None
        assert ds["verification"]["status"] == "never"
        assert "prune" not in ds

    def test_label_overrides_name(self, monkeypatch):
        adapter = _make_adapter(
            monkeypatch, label="Home PBS", datastores=["ssd"],
        )
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["name"] == "Home PBS"

    def test_name_defaults_to_store(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 100, "used": 50, "avail": 50,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["name"] == "ssd"

    def test_source_type_and_id_set(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
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
            **_phase6_empty_defaults("ssd", "broken"),
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
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 1024**3 * 10, "used": 1024**3 * 5,
                "avail": 1024**3 * 5,
            }),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["size_gb"] == 10.0

    def test_backup_count_sums_groups_across_namespaces(self, monkeypatch):
        """`_count_backups` sums `backup-count` across every group in every
        namespace. The overview card uses this instead of the hardcoded 0."""
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 200, "used": 100, "avail": 100,
            }),
            # Override the empty namespace/groups defaults.
            "/admin/datastore/ssd/namespace": _FakeResp(
                [{"ns": ""}, {"ns": "prod"}],
            ),
            "/admin/datastore/ssd/groups": _FakeResp([
                {"backup-type": "vm", "backup-id": "100", "backup-count": 5},
                {"backup-type": "ct", "backup-id": "200", "backup-count": 3},
            ]),
        })
        [ds] = adapter.fetch_datastores()
        # Two namespaces × two groups × (5 + 3) each = 16.
        assert ds["metrics"]["backup_count"] == 16

    def test_backup_count_falls_back_to_zero_on_api_error(
        self, monkeypatch, capsys,
    ):
        """Transport errors against /namespace must not break the overview —
        same resilience pattern as GC/verify. backup_count falls to 0 and a
        log line surfaces the reason."""
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp({
                "total": 200, "used": 100, "avail": 100,
            }),
            "/admin/datastore/ssd/namespace":
                requests.ConnectionError("ns offline"),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["metrics"]["backup_count"] == 0
        assert "namespaces" in capsys.readouterr().out


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

    def test_files_and_verification_propagate_to_snapshot(self, monkeypatch):
        """The PBS /snapshots payload includes `files` and `verification`
        inline — both must flow through `build_snapshot_record` so the WebUI
        backup browser can render the file table and the per-snapshot
        verify pill."""
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            "/admin/datastore/ssd/namespace": _FakeResp([{"ns": ""}]),
            "/admin/datastore/ssd/groups": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 400,
                "last-backup": 1700000000,
                "backup-count": 1,
            }]),
            "/admin/datastore/ssd/snapshots": _FakeResp([{
                "backup-type": "vm",
                "backup-id": 400,
                "backup-time": 1700000000,
                "size": 2048,
                "protected": False,
                "files": [
                    {"filename": "qemu-server.conf.blob", "size": 495},
                    {"filename": "drive-scsi0.img.fidx", "size": 1024**3},
                ],
                "verification": {
                    "state": "ok",
                    "upid": "UPID:node:...",
                },
            }]),
        })
        inv = adapter.fetch_backup_inventory(
            "pbs:home-lab:ssd", snapshot_cap=50,
        )
        [grp] = inv["groups"]
        [snap] = grp["current_snapshots"]
        assert snap["files"] == [
            {"filename": "qemu-server.conf.blob", "size": 495},
            {"filename": "drive-scsi0.img.fidx", "size": 1024**3},
        ]
        assert snap["verification"] == {
            "state": "ok", "upid": "UPID:node:...",
        }

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


def _ok_gc_task(starttime=1700000000, endtime=1700003600):
    return {
        "upid": "UPID:pbs:1234:gc:ssd:1",
        "worker_id": "ssd",
        "status": "OK",
        "starttime": starttime,
        "endtime": endtime,
        "type": "garbage_collection",
    }


def _ok_verify_task(starttime=1700050000, endtime=1700060000):
    return {
        "upid": "UPID:pbs:5678:verify:ssd:1",
        "worker_id": "ssd:v-abc",
        "status": "OK",
        "starttime": starttime,
        "endtime": endtime,
        "type": "verificationjob",
    }


def _tasks_variants(gc_tasks, verify_tasks):
    """Build a `_variants` entry for /nodes/<node>/tasks that dispatches on
    the `typefilter` param."""
    return {
        "_variants": [
            {
                "params": {"typefilter": "garbage_collection"},
                "response": _FakeResp(gc_tasks),
            },
            {
                "params": {"typefilter": "verificationjob"},
                "response": _FakeResp(verify_tasks),
            },
        ],
    }


class TestFetchGc:
    def test_last_task_ok(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants(
                [_ok_gc_task(starttime=1792818581)], [],
            ),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "ok"
        assert ds["gc"]["last_run"].startswith("2026")  # ISO timestamp

    def test_last_task_error(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        failing = _ok_gc_task()
        failing["status"] = "connection error: boom"
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([failing], []),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "error"

    def test_no_tasks_means_never(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "never"
        assert ds["gc"]["last_run"] is None

    def test_next_scheduled_from_gc_schedule(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({"gc-schedule": "daily"}),
            "/tasks": _tasks_variants([], []),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["next_scheduled"] is not None

    def test_no_schedule_means_next_none(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["next_scheduled"] is None

    def test_malformed_schedule_falls_back_to_none(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({"gc-schedule": "garbage"}),
            "/tasks": _tasks_variants([], []),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["next_scheduled"] is None

    def test_running_task_without_endtime_is_ignored(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        running = _ok_gc_task()
        running["endtime"] = None
        running["status"] = None
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([running], []),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "never"


class TestFetchVerification:
    def test_last_task_ok(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([], [_ok_verify_task()]),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["verification"]["status"] == "ok"

    def test_last_task_error(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        failing = _ok_verify_task()
        failing["status"] = "integrity failure"
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([], [failing]),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["verification"]["status"] == "error"

    def test_verify_new_from_datastore_config(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({"verify-new": True}),
            "/tasks": _tasks_variants([], []),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["verification"]["verify_new"] is True

    def test_schedule_from_config_verify_filtered_by_store(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([], []),
            "/config/verify": _FakeResp([
                {
                    "id": "v-other", "store": "cold", "ns": "",
                    "schedule": "weekly", "outdated-after": 7,
                },
                {
                    "id": "v-mine", "store": "ssd", "ns": "",
                    "schedule": "daily", "outdated-after": 30,
                },
            ]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["verification"]["outdated_after_days"] == 30
        assert ds["verification"]["next_scheduled"] is not None

    def test_outdated_after_missing_is_none(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": _tasks_variants([], []),
            "/config/verify": _FakeResp([
                {"id": "v", "store": "ssd", "ns": "", "schedule": "daily"},
            ]),
            "/config/prune": _FakeResp([]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["verification"]["outdated_after_days"] is None


class TestFetchPrune:
    def test_single_root_ns_job_flat_shape(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/prune": _FakeResp([{
                "id": "s-1", "store": "ssd", "ns": "",
                "schedule": "hourly",
                "keep-last": 4, "keep-hourly": 6, "keep-daily": 7,
                "keep-weekly": 6, "keep-monthly": 12, "keep-yearly": 2,
            }]),
        })
        [ds] = adapter.fetch_datastores()
        prune = ds["prune"]
        assert prune["schedule"] == "hourly"
        assert prune["keep_last"] == 4
        assert prune["keep_hourly"] == 6
        assert prune["keep_daily"] == 7
        assert prune["keep_weekly"] == 6
        assert prune["keep_monthly"] == 12
        assert prune["keep_yearly"] == 2
        assert len(prune["jobs"]) == 1
        assert prune["jobs"][0]["ns"] == ""

    def test_multi_ns_root_wins_as_primary(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/prune": _FakeResp([
                {
                    "id": "s-prod", "store": "ssd", "ns": "prod",
                    "schedule": "weekly", "keep-last": 1,
                },
                {
                    "id": "s-root", "store": "ssd", "ns": "",
                    "schedule": "daily", "keep-last": 5,
                },
            ]),
        })
        [ds] = adapter.fetch_datastores()
        prune = ds["prune"]
        # Root-ns job wins the flat shape even though it's listed second.
        assert prune["schedule"] == "daily"
        assert prune["keep_last"] == 5
        assert len(prune["jobs"]) == 2
        assert {j["ns"] for j in prune["jobs"]} == {"", "prod"}

    def test_multi_ns_no_root_first_wins(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/prune": _FakeResp([
                {
                    "id": "s-prod", "store": "ssd", "ns": "prod",
                    "schedule": "weekly", "keep-last": 1,
                },
                {
                    "id": "s-dev", "store": "ssd", "ns": "dev",
                    "schedule": "monthly", "keep-last": 2,
                },
            ]),
        })
        [ds] = adapter.fetch_datastores()
        prune = ds["prune"]
        assert prune["schedule"] == "weekly"
        assert prune["keep_last"] == 1

    def test_no_prune_jobs_for_store_omits_field(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/prune": _FakeResp([
                {
                    "id": "s-other", "store": "cold", "ns": "",
                    "schedule": "weekly",
                },
            ]),
        })
        [ds] = adapter.fetch_datastores()
        assert "prune" not in ds

    def test_missing_keep_fields_default_to_none(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_phase6_empty_defaults("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/prune": _FakeResp([{
                "id": "s-partial", "store": "ssd", "ns": "",
                "schedule": "daily",
                "keep-last": 3,  # keep-weekly/monthly/yearly omitted
            }]),
        })
        [ds] = adapter.fetch_datastores()
        prune = ds["prune"]
        assert prune["keep_last"] == 3
        assert prune["keep_weekly"] is None
        assert prune["keep_monthly"] is None
        assert prune["keep_yearly"] is None


class TestFetchDatastoresIntegratesGcVerifyPrune:
    def _baseline_session(self):
        return _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({
                "gc-schedule": "daily",
                "verify-new": False,
            }),
            "/tasks": _tasks_variants(
                [_ok_gc_task(starttime=1792818581)],
                [_ok_verify_task(starttime=1792900000)],
            ),
            "/config/verify": _FakeResp([
                {
                    "id": "v-ssd", "store": "ssd", "ns": "",
                    "schedule": "daily", "outdated-after": 30,
                },
            ]),
            "/config/prune": _FakeResp([{
                "id": "s-ssd", "store": "ssd", "ns": "",
                "schedule": "hourly",
                "keep-last": 4, "keep-hourly": 6, "keep-daily": 7,
                "keep-weekly": 6, "keep-monthly": 12, "keep-yearly": 2,
            }]),
        })

    def test_all_three_sections_populated(self, monkeypatch):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = self._baseline_session()
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "ok"
        assert ds["gc"]["next_scheduled"] is not None
        assert ds["verification"]["status"] == "ok"
        assert ds["verification"]["outdated_after_days"] == 30
        assert ds["prune"]["keep_last"] == 4
        assert ds["prune"]["schedule"] == "hourly"

    def test_task_endpoint_error_surfaces_in_gc_and_verification(
        self, monkeypatch, capsys,
    ):
        adapter = _make_adapter(monkeypatch, datastores=["ssd"])
        adapter._session_cache = _FakeSession({
            **_backup_count_mocks("ssd"),
            "/admin/datastore/ssd/status": _FakeResp(
                {"total": 100, "used": 50, "avail": 50},
            ),
            "/config/datastore/ssd": _FakeResp({}),
            "/tasks": requests.ConnectionError("tasks endpoint down"),
            "/config/verify": _FakeResp([]),
            "/config/prune": _FakeResp([{
                "id": "s-ssd", "store": "ssd", "ns": "",
                "schedule": "hourly", "keep-last": 4,
            }]),
        })
        [ds] = adapter.fetch_datastores()
        assert ds["gc"]["status"] == "error"
        assert "tasks endpoint down" in ds["gc"]["error"]
        assert ds["verification"]["status"] == "error"
        assert "tasks endpoint down" in ds["verification"]["error"]
        assert ds["prune"]["keep_last"] == 4
        captured = capsys.readouterr()
        assert "GC status" in captured.out
        assert "verification status" in captured.out


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


class TestNodeValidation:
    @pytest.mark.parametrize("bad_node", [
        "pbs.fmaschke.de",
        "https://pbs",
        "pbs/node",
    ])
    def test_rejects_fqdn_or_url_node(self, monkeypatch, bad_node):
        monkeypatch.setenv("PBS_TOKEN", "tok")
        with pytest.raises(ValueError, match="short hostname"):
            PbsAdapter(
                source_id="bad",
                base_url="https://pbs.example.com:8007",
                node=bad_node,
                token_env="PBS_TOKEN",
            )

