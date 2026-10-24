"""Adapter for a local Proxmox Backup Server (PBS) instance.

Implements the SourceAdapter protocol against the native PBS HTTP API
(`/api2/json/...`). Phase 4–5 scope — see
temp/pbs-integration-multi-phase-plan.md:

- `check_health()` via GET /version
- `fetch_datastores()` via GET /admin/datastore (auto-discovery or explicit
   list) + GET /admin/datastore/{store}/status (metrics)
- `fetch_backup_inventory()` via GET /admin/datastore/{store}/namespace +
   .../groups?ns=… + .../snapshots?ns=…, normalised through the shared
   cloud-shape helpers in alerting.monitor
- `fetch_rescale_log()` — always returns [] (PBS has no plan-rescale concept)

Auth header: `Authorization: PBSAPIToken=<USER@REALM!TOKENID:SECRET>`.
TLS: standard CA validation by default; optional SHA-256 fingerprint pinning
via urllib3's native `assert_fingerprint` parameter.
"""

from __future__ import annotations

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from .base import HealthResult, SourceConnectionError, diagnose_network


class _FingerprintAdapter(HTTPAdapter):
    """HTTPAdapter that pins the server's SHA-256 certificate fingerprint.

    Delegates the actual check to urllib3's `assert_fingerprint`, which is
    permissive about colons/case. We normalise the input and validate it is
    64 hex characters before constructing the adapter so that configuration
    errors surface at startup, not at first request.
    """

    def __init__(self, fingerprint: str, **kwargs):
        normalised = fingerprint.replace(":", "").lower()
        if len(normalised) != 64 or any(c not in "0123456789abcdef" for c in normalised):
            raise ValueError(
                f"fingerprint must be a 64-character SHA-256 hex string "
                f"(got {len(normalised)} chars after normalisation)"
            )
        self._fingerprint = normalised
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["assert_fingerprint"] = self._fingerprint
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )


class PbsAdapter:
    """SourceAdapter implementation for a Proxmox Backup Server instance."""

    def __init__(
        self,
        source_id: str,
        base_url: str,
        node: str,
        *,
        token_env: str,
        label: str | None = None,
        fingerprint: str | None = None,
        verify_tls: bool = True,
        datastores: list[str] | None = None,
    ):
        self._source_id = source_id
        self._base_url = base_url.rstrip("/")
        self._node = node
        self._token_env = token_env
        self._label = label
        self._fingerprint = fingerprint
        self._verify_tls = verify_tls
        self._datastores = list(datastores) if datastores else []
        self._session_cache: requests.Session | None = None

    @property
    def source_id(self) -> str:
        return self._source_id

    @property
    def base_url(self) -> str:
        return self._base_url

    def _session(self) -> requests.Session:
        if self._session_cache is not None:
            return self._session_cache
        sess = requests.Session()
        if self._fingerprint:
            sess.mount("https://", _FingerprintAdapter(self._fingerprint))
            sess.verify = False
        else:
            sess.verify = self._verify_tls
        self._session_cache = sess
        return sess

    def _api_get(self, path: str, params=None):
        """Authenticated GET against the PBS API, returning the `data` payload.

        Raises:
            RuntimeError: when the token env var is unset (matches the
                RemoteBackupsAdapter contract so run_check's except-branch
                still works unchanged).
            SourceConnectionError: on TLS fingerprint mismatch.
            requests.RequestException: on other HTTP/network failures.
        """
        token = os.environ.get(self._token_env, "")
        if not token:
            raise RuntimeError(
                f"{self._token_env} not set in environment or .env"
            )
        headers = {"Authorization": f"PBSAPIToken={token}"}
        try:
            resp = self._session().get(
                f"{self._base_url}{path}",
                headers=headers,
                params=params,
                timeout=15,
            )
        except requests.exceptions.SSLError as e:
            raise SourceConnectionError(
                f"TLS verification failed for {self._base_url}: {e}"
            ) from e
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data") if isinstance(payload, dict) else payload

    def check_health(self) -> HealthResult:
        """Hit /version. 401/403 → degraded; network error → down."""
        try:
            data = self._api_get("/api2/json/version") or {}
            version = data.get("version", "?")
            release = data.get("release", "")
            message = f"PBS {version}-{release}" if release else f"PBS {version}"
            return HealthResult(status="ok", message=message)
        except RuntimeError as e:
            return HealthResult(status="degraded", message=str(e), error=e)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            if code in (401, 403):
                return HealthResult(
                    status="degraded",
                    message=f"PBS auth failed (HTTP {code})",
                    error=e,
                )
            return HealthResult(
                status="down",
                message=f"HTTP {code}",
                error=e,
            )
        except (SourceConnectionError, requests.RequestException) as e:
            return HealthResult(
                status="down",
                message=str(e),
                net_issue=diagnose_network(self._base_url),
                error=e,
            )

    def fetch_datastores(self) -> list[dict]:
        """List datastores and normalise their status to the internal shape.

        Explicit `datastores` from config short-circuits auto-discovery.
        Per-store status failures are logged and skip that store (metrics
        are mandatory downstream for storage alerts).
        """
        if self._datastores:
            store_names = self._datastores
        else:
            discovery = self._api_get("/api2/json/admin/datastore") or []
            store_names = [
                entry.get("store")
                for entry in discovery
                if isinstance(entry, dict) and entry.get("store")
            ]

        result: list[dict] = []
        for store in store_names:
            try:
                status = self._api_get(
                    f"/api2/json/admin/datastore/{store}/status"
                ) or {}
            except (requests.RequestException, SourceConnectionError) as e:
                print(
                    f"PBS source {self._source_id!r}: failed to fetch status "
                    f"for datastore {store!r}: {e}"
                )
                continue
            result.append(self._normalise_status(store, status))
        return result

    def _normalise_status(self, store: str, status: dict) -> dict:
        total = int(status.get("total") or 0)
        used = int(status.get("used") or 0)
        avail = int(status.get("avail") or 0)
        used_percent = round(used / total * 100, 1) if total > 0 else 0.0
        size_gb = round(total / 1024**3, 2) if total > 0 else 0.0
        return {
            "id": f"pbs:{self._source_id}:{store}",
            "name": self._label or store,
            "source_type": "pbs",
            "source_id": self._source_id,
            "size_gb": size_gb,
            "metrics": {
                "used_bytes": used,
                "available_bytes": avail,
                "used_percent": used_percent,
                "backup_count": 0,
            },
        }

    def fetch_backup_inventory(
        self, datastore_id: str, *, snapshot_cap: int
    ) -> dict:
        """Fetch namespaces/groups/snapshots and normalise to cloud shape.

        PBS returns dash-separated field names (`backup-type`, `backup-id`,
        `backup-time`, `last-backup`, `backup-count`); we translate them to
        the underscore-based cloud shape so the shared helpers
        `extract_namespace_backup_groups`/`build_backup_group_record` in
        alerting.monitor can produce byte-identical records regardless of
        source.
        """
        from alerting.monitor import extract_namespace_backup_groups

        store = _parse_pbs_datastore_id(datastore_id)

        namespaces = self._api_get(
            f"/api2/json/admin/datastore/{store}/namespace"
        ) or []
        namespace_values = [
            ns.get("ns", "") for ns in namespaces if isinstance(ns, dict)
        ]
        if not namespace_values:
            namespace_values = [""]
        namespace_values = sorted(
            namespace_values, key=lambda v: (v != "", v)
        )

        groups: list[dict] = []
        for ns in namespace_values:
            params = {"ns": ns} if ns else None
            pbs_groups = self._api_get(
                f"/api2/json/admin/datastore/{store}/groups", params=params
            ) or []
            pbs_snapshots = self._api_get(
                f"/api2/json/admin/datastore/{store}/snapshots", params=params
            ) or []
            namespace_data = {
                "groups": [
                    _pbs_to_cloud_keys(g)
                    for g in pbs_groups if isinstance(g, dict)
                ],
                "snapshots": [
                    _pbs_to_cloud_keys(s)
                    for s in pbs_snapshots if isinstance(s, dict)
                ],
            }
            groups.extend(
                extract_namespace_backup_groups(
                    ns, namespace_data, snapshot_cap
                )
            )

        snapshot_count = sum(g["backup_count"] for g in groups)
        return {
            "summary": {
                "namespace_count": len(namespace_values),
                "group_count": len(groups),
                "snapshot_count": snapshot_count,
            },
            "groups": groups,
        }

    def fetch_rescale_log(self, datastore_id: str) -> list[dict]:
        return []


def _pbs_to_cloud_keys(item: dict) -> dict:
    """Translate dash-separated PBS field names to underscore cloud shape."""
    return {k.replace("-", "_"): v for k, v in item.items()}


def _parse_pbs_datastore_id(datastore_id: str) -> str:
    """Extract the PBS store name from a `pbs:<source>:<store>` id."""
    parts = datastore_id.split(":", 2)
    if len(parts) < 3 or parts[0] != "pbs" or not parts[2]:
        raise ValueError(
            f"PbsAdapter: expected 'pbs:<source>:<store>' datastore_id, "
            f"got {datastore_id!r}"
        )
    return parts[2]
