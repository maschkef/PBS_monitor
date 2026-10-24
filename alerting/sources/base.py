"""Source-adapter base contracts.

Defines the Protocol every datastore source (remote-backups.com cloud, local
PBS, …) implements, plus shared primitives (HealthResult, exceptions,
network-diagnosis helper).

The adapter pattern is introduced in phase 1 of the PBS multi-source plan as
pure refactor; behaviour of the only existing adapter (RemoteBackupsAdapter)
is byte-identical to the pre-refactor inline logic in alerting/monitor.py.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable
from urllib.parse import urlparse

HealthStatus = Literal["ok", "degraded", "down"]


@dataclass
class HealthResult:
    """Result of SourceAdapter.check_health().

    `status` drives the alert decision in the monitor loop:
      - "ok": no action
      - "degraded": raise an "API Unhealthy"-style alert
      - "down": raise an "API Unreachable"-style alert and bail out of the cycle
    `message` holds a short human-readable detail. `net_issue` carries the
    local-network diagnosis (DNS/TCP) when applicable so the caller can print
    a hint. `error` keeps the original exception for formatting.
    """

    status: HealthStatus
    message: str | None = None
    net_issue: str | None = None
    error: Exception | None = None


class SourceError(Exception):
    """Base class for errors raised by source adapters."""


class SourceConnectionError(SourceError):
    """Network / TLS / timeout failure talking to the source."""


class SourceAuthError(SourceError):
    """Missing or invalid credentials for the source."""


@runtime_checkable
class SourceAdapter(Protocol):
    """Interface every datastore source must implement."""

    @property
    def source_id(self) -> str: ...

    def check_health(self) -> HealthResult: ...

    def fetch_datastores(self) -> list[dict]: ...

    def fetch_backup_inventory(
        self, datastore_id: str, *, snapshot_cap: int
    ) -> dict: ...

    def fetch_rescale_log(self, datastore_id: str) -> list[dict]: ...


def diagnose_network(url: str) -> str | None:
    """Return a human-readable local-network issue string, or None if the
    network path to `url` looks fine. Checks DNS resolution and a TCP open.
    """
    parsed = urlparse(url)
    host = parsed.hostname or url
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        socket.getaddrinfo(host, port)
    except socket.gaierror as e:
        return f"DNS resolution failed for '{host}': {e}"
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as e:
        return f"TCP connection to {host}:{port} failed: {e}"
    return None
