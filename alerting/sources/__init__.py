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
from .pbs import PbsAdapter
from .registry import build_source, validate_source_entries
from .remote_backups import RemoteBackupsAdapter

__all__ = [
    "HealthResult",
    "HealthStatus",
    "PbsAdapter",
    "RemoteBackupsAdapter",
    "SourceAdapter",
    "SourceAuthError",
    "SourceConnectionError",
    "SourceError",
    "build_source",
    "diagnose_network",
    "validate_source_entries",
]
