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
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from .base import HealthResult, SourceConnectionError, diagnose_network
from .pbs_calendar import pbs_calendar_to_next_utc


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
        ds_dict: dict = {
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

        datastore_config = self._fetch_section_safe(
            f"/api2/json/config/datastore/{store}", store, "datastore config",
        )
        if not isinstance(datastore_config, dict):
            datastore_config = {}

        gc = self._fetch_section_safe(
            lambda: self._fetch_gc(store, datastore_config),
            store, "GC status",
        )
        if gc:
            ds_dict["gc"] = gc

        verification = self._fetch_section_safe(
            lambda: self._fetch_verification(store, datastore_config),
            store, "verification status",
        )
        if verification:
            ds_dict["verification"] = verification

        prune = self._fetch_section_safe(
            lambda: self._fetch_prune(store),
            store, "prune config",
        )
        if prune:
            ds_dict["prune"] = prune

        return ds_dict

    def _fetch_section_safe(self, fetcher, store: str, label: str):
        """Call ``fetcher`` (either a path or a zero-arg callable) and swallow
        transport errors with a log line. Returns None on failure so callers
        can decide whether to omit the field.
        """
        try:
            if callable(fetcher):
                return fetcher()
            return self._api_get(fetcher)
        except (requests.RequestException, SourceConnectionError) as e:
            print(
                f"PBS source {self._source_id!r}: failed to fetch {label} "
                f"for datastore {store!r}: {e}"
            )
            return None

    def _fetch_last_task(self, store: str, typefilter: str) -> dict | None:
        """Return the most-recent finished task of a given typefilter.

        PBS filters server-side on both ``store`` and ``typefilter``; passing
        ``running=false`` excludes in-flight tasks so we never surface a
        half-complete job as "last run". The response is already ordered
        newest-first, but we sort defensively by ``starttime`` to tolerate
        future API changes.
        """
        params = {
            "typefilter": typefilter,
            "store": store,
            "running": "false",
            "limit": 10,
        }
        tasks = self._api_get(
            f"/api2/json/nodes/{self._node}/tasks", params=params,
        ) or []
        if not isinstance(tasks, list):
            return None
        finished = [
            t for t in tasks
            if isinstance(t, dict) and t.get("endtime") is not None
        ]
        if not finished:
            return None
        finished.sort(
            key=lambda t: t.get("starttime") or 0, reverse=True,
        )
        return finished[0]

    def _fetch_gc(self, store: str, datastore_config: dict) -> dict:
        last_task = self._fetch_last_task(store, "garbage_collection")
        status = _map_task_status(last_task)
        last_run = _task_starttime_iso(last_task)
        schedule = datastore_config.get("gc-schedule")
        next_scheduled = _schedule_next_iso(schedule)
        return {
            "status": status,
            "last_run": last_run,
            "next_scheduled": next_scheduled,
        }

    def _fetch_verification(
        self, store: str, datastore_config: dict,
    ) -> dict:
        last_task = self._fetch_last_task(store, "verificationjob")
        status = _map_task_status(last_task)
        last_run = _task_starttime_iso(last_task)

        verify_jobs = self._fetch_section_safe(
            "/api2/json/config/verify", store, "verify jobs",
        ) or []
        if not isinstance(verify_jobs, list):
            verify_jobs = []
        store_jobs = [
            job for job in verify_jobs
            if isinstance(job, dict) and job.get("store") == store
        ]
        primary = store_jobs[0] if store_jobs else {}
        next_scheduled = _schedule_next_iso(primary.get("schedule"))
        outdated_after = primary.get("outdated-after")
        try:
            outdated_after_days = (
                int(outdated_after) if outdated_after is not None else None
            )
        except (TypeError, ValueError):
            outdated_after_days = None

        verify_new_raw = datastore_config.get("verify-new")
        verify_new = bool(verify_new_raw) if verify_new_raw is not None else False

        return {
            "status": status,
            "last_run": last_run,
            "next_scheduled": next_scheduled,
            "outdated_after_days": outdated_after_days,
            "verify_new": verify_new,
        }

    def _fetch_prune(self, store: str) -> dict:
        prune_jobs = self._api_get("/api2/json/config/prune")
        if not isinstance(prune_jobs, list):
            return {}
        store_jobs = [
            job for job in prune_jobs
            if isinstance(job, dict) and job.get("store") == store
        ]
        if not store_jobs:
            return {}

        normalised = [_normalise_prune_job(job) for job in store_jobs]
        primary = next(
            (j for j in normalised if j.get("ns") == ""),
            normalised[0],
        )
        return {
            "schedule": primary.get("schedule"),
            "keep_last": primary.get("keep_last"),
            "keep_hourly": primary.get("keep_hourly"),
            "keep_daily": primary.get("keep_daily"),
            "keep_weekly": primary.get("keep_weekly"),
            "keep_monthly": primary.get("keep_monthly"),
            "keep_yearly": primary.get("keep_yearly"),
            "jobs": normalised,
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


def _map_task_status(task: dict | None) -> str:
    """Translate a PBS task record to the internal gc/verify status enum.

    - No task → ``"never"``
    - ``status == "OK"`` → ``"ok"``
    - Any other non-empty status → ``"error"``
    - Missing status on a task that _fetch_last_task returned → ``"ok"``
      (shouldn't normally happen since we filter by endtime upstream)
    """
    if task is None:
        return "never"
    status = (task.get("status") or "").strip()
    if not status:
        return "ok"
    if status == "OK":
        return "ok"
    return "error"


def _task_starttime_iso(task: dict | None) -> str | None:
    if task is None:
        return None
    starttime = task.get("starttime")
    if starttime is None:
        return None
    try:
        return datetime.fromtimestamp(
            int(starttime), tz=UTC,
        ).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _schedule_next_iso(schedule: str | None) -> str | None:
    if not schedule:
        return None
    nxt = pbs_calendar_to_next_utc(schedule)
    return nxt.isoformat() if nxt is not None else None


_PRUNE_KEEP_KEYS = (
    "keep_last", "keep_hourly", "keep_daily",
    "keep_weekly", "keep_monthly", "keep_yearly",
)


def _normalise_prune_job(job: dict) -> dict:
    """Translate a PBS /config/prune entry to the internal underscore shape.

    Missing ``keep-*`` fields map to ``None`` (not ``0``) so downstream code
    can distinguish "no retention bucket configured" from "retain zero".
    """
    translated = _pbs_to_cloud_keys(job)
    result: dict = {
        "ns": translated.get("ns", "") or "",
        "schedule": translated.get("schedule"),
    }
    for key in _PRUNE_KEEP_KEYS:
        raw = translated.get(key)
        if raw is None:
            result[key] = None
            continue
        try:
            result[key] = int(raw)
        except (TypeError, ValueError):
            result[key] = None
    return result
