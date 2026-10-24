"""Adapter for the remote-backups.com Monitoring API.

Encapsulates all HTTP calls that used to live in alerting/monitor.py as
module-level functions (api_get / fetch_datastores / fetch_backup_inventory /
fetch_rescale_log) plus the previously-inline /health probe. Semantics are
byte-identical to the pre-refactor code — see phase 1 of the multi-source
plan in temp/pbs-integration-multi-phase-plan.md.
"""

from __future__ import annotations

import os

import requests

from .base import HealthResult, diagnose_network


class RemoteBackupsAdapter:
    """SourceAdapter implementation for api.remote-backups.com."""

    def __init__(self, source_id: str, api_base: str, *, api_key_env: str = "API_KEY"):
        self._source_id = source_id
        self._api_base = api_base
        self._api_key_env = api_key_env

    @property
    def source_id(self) -> str:
        return self._source_id

    @property
    def api_base(self) -> str:
        return self._api_base

    def _api_get(self, path, params=None):
        """Authenticated GET against the Monitoring API.

        Raises RuntimeError when the configured API-key env var is unset
        (preserving the original contract so callers in alerting/monitor.py
        that catch RuntimeError continue to work unchanged).
        """
        api_key = os.environ.get(self._api_key_env, "")
        if not api_key:
            raise RuntimeError(f"{self._api_key_env} not set in environment or .env")
        headers = {"Authorization": f"Bearer {api_key}"}
        resp = requests.get(
            f"{self._api_base}{path}",
            headers=headers,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def check_health(self) -> HealthResult:
        """Hit the public /health endpoint (unauthenticated, 10s timeout)."""
        try:
            health = requests.get(
                f"{self._api_base}/health", timeout=10
            ).json()
            status = health.get("status", "unknown")
            if status == "ok":
                return HealthResult(status="ok", message=status)
            return HealthResult(status="degraded", message=status)
        except requests.RequestException as e:
            return HealthResult(
                status="down",
                message=str(e),
                net_issue=diagnose_network(self._api_base),
                error=e,
            )

    def fetch_datastores(self) -> list[dict]:
        """Fetch all datastores with per-datastore details.

        Falls back to the list-only record for a datastore if the detail
        request fails — matches the original behaviour.
        """
        datastores = self._api_get("/monitoring/v1/datastores")
        result = []
        for ds in datastores:
            try:
                detail = self._api_get(f"/monitoring/v1/datastores/{ds['id']}")
                result.append(detail)
            except requests.RequestException:
                result.append(ds)
        return result

    def fetch_backup_inventory(
        self, datastore_id: str, *, snapshot_cap: int
    ) -> dict:
        """Fetch full backup inventory for a datastore, grouped by namespace.

        `snapshot_cap` controls how many recent snapshots per group are
        materialised. The caller computes it from the config (previously
        inline in the pre-refactor fetch_backup_inventory).
        """
        from alerting.monitor import extract_namespace_backup_groups

        base_path = f"/monitoring/v1/datastores/{datastore_id}/backups"
        overview = self._api_get(base_path)
        namespace_entries = overview.get("namespaces") or [
            {"ns": "", "comment": None}
        ]
        namespace_entries = sorted(
            namespace_entries,
            key=lambda item: (item.get("ns", "") != "", item.get("ns", "")),
        )

        groups = []
        for namespace_meta in namespace_entries:
            namespace_value = namespace_meta.get("ns", "")
            namespace_data = self._api_get(
                base_path, params={"ns": namespace_value}
            )
            groups.extend(
                extract_namespace_backup_groups(
                    namespace_value, namespace_data, snapshot_cap
                )
            )

        snapshot_count = sum(group["backup_count"] for group in groups)
        return {
            "summary": {
                "namespace_count": len(namespace_entries),
                "group_count": len(groups),
                "snapshot_count": snapshot_count,
            },
            "groups": groups,
        }

    def fetch_rescale_log(self, datastore_id: str) -> list[dict]:
        """Fetch the recent rescale-log entries (upstream default window: 30d)."""
        entries = self._api_get(
            f"/monitoring/v1/datastores/{datastore_id}/rescale-log",
            params={"range": "30d"},
        )
        return entries if isinstance(entries, list) else []
