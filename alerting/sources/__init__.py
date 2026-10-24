"""Datastore source adapters.

Public re-exports so callers can `from alerting.sources import …` without
reaching into submodules.
"""

from .base import (
    HealthResult,
    HealthStatus,
    SourceAdapter,
    SourceAuthError,
    SourceConnectionError,
    SourceError,
    diagnose_network,
)
from .registry import build_source
from .remote_backups import RemoteBackupsAdapter

__all__ = [
    "HealthResult",
    "HealthStatus",
    "RemoteBackupsAdapter",
    "SourceAdapter",
    "SourceAuthError",
    "SourceConnectionError",
    "SourceError",
    "build_source",
    "diagnose_network",
]
