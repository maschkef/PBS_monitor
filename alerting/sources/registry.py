"""Factory for building SourceAdapter instances from config entries.

Phase 1 only knows the remote-backups.com adapter; phase 4 will extend this
with a "pbs" type for local Proxmox Backup Server instances.
"""

from __future__ import annotations

from .base import SourceAdapter
from .remote_backups import RemoteBackupsAdapter


def build_source(cfg_entry: dict) -> SourceAdapter:
    """Build a SourceAdapter from a `sources: []` config entry.

    Phase 1 config entries have the shape:
        {"id": "<slug>", "type": "remote_backups", "api_base": "<url>"}
    """
    source_id = cfg_entry.get("id") or "default"
    stype = cfg_entry.get("type", "remote_backups")
    if stype == "remote_backups":
        return RemoteBackupsAdapter(
            source_id=source_id,
            api_base=cfg_entry["api_base"],
        )
    raise ValueError(f"Unknown source type: {stype!r}")
