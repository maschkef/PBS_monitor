"""Factory and validator for building SourceAdapter instances from config.

Phase 3 introduces `validate_source_entries` so `load_config()` can normalise a
`sources: []` config list (synthesising a default remote-backups entry when
empty) and validate every entry's identity/type before any adapter is built.

Phase 4 will extend `build_source` with a real PBS adapter; today the "pbs"
type is only accepted by the validator so a user can author forward-looking
config entries, but `build_source` still refuses to construct one.
"""

from __future__ import annotations

import re

from .base import SourceAdapter
from .remote_backups import RemoteBackupsAdapter

_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_VALID_TYPES = frozenset({"remote_backups", "pbs"})


def validate_source_entries(
    sources, *, legacy_api_base: str | None = None,
) -> list[dict]:
    """Validate and normalise a `sources: []` config list.

    An empty / missing list synthesises a single implicit remote-backups
    source from the legacy top-level `api_base`, preserving pre-phase-3
    behaviour. Returns a fresh list of validated dicts; raises `ValueError`
    with a message pointing at the offending entry index on failure.
    """
    if sources is None or sources == []:
        if not legacy_api_base:
            raise ValueError(
                "no sources configured and legacy api_base is missing"
            )
        return [{
            "id": "default",
            "type": "remote_backups",
            "api_base": legacy_api_base,
            "api_key_env": "API_KEY",
        }]

    if not isinstance(sources, list):
        raise ValueError("sources must be a list")  # noqa: TRY004

    seen_ids: set[str] = set()
    validated: list[dict] = []
    for i, entry in enumerate(sources):
        if not isinstance(entry, dict):
            raise ValueError(f"sources[{i}] must be an object")  # noqa: TRY004

        sid = entry.get("id")
        if not isinstance(sid, str) or not _ID_PATTERN.match(sid):
            raise ValueError(
                f"sources[{i}].id must match {_ID_PATTERN.pattern!r} "
                f"(got {sid!r})"
            )
        if sid in seen_ids:
            raise ValueError(f"sources[{i}].id duplicates earlier id {sid!r}")
        seen_ids.add(sid)

        stype = entry.get("type")
        if stype not in _VALID_TYPES:
            raise ValueError(
                f"sources[{i}].type must be one of {sorted(_VALID_TYPES)} "
                f"(got {stype!r})"
            )

        normalised = dict(entry)
        if stype == "remote_backups":
            api_base = entry.get("api_base")
            if not isinstance(api_base, str) or not api_base:
                raise ValueError(
                    f"sources[{i}].api_base must be a non-empty string "
                    f"for type 'remote_backups'"
                )
            normalised.setdefault("api_key_env", "API_KEY")
        # Phase 4 will add validation for pbs-specific fields (base_url,
        # node, token_env, …). For now any pbs entry is accepted so users
        # can author forward-looking configs; build_source still refuses.

        validated.append(normalised)
    return validated


def build_source(cfg_entry: dict) -> SourceAdapter:
    """Build a SourceAdapter from a `sources: []` config entry.

    Phase 3 accepts the following shapes:
        {"id": "<slug>", "type": "remote_backups",
         "api_base": "<url>", "api_key_env": "API_KEY"}
        {"id": "<slug>", "type": "pbs", …}  # raises until phase 4
    """
    source_id = cfg_entry.get("id") or "default"
    stype = cfg_entry.get("type", "remote_backups")
    if stype == "remote_backups":
        return RemoteBackupsAdapter(
            source_id=source_id,
            api_base=cfg_entry["api_base"],
            api_key_env=cfg_entry.get("api_key_env", "API_KEY"),
        )
    if stype == "pbs":
        raise ValueError("PBS source adapter not yet implemented (Phase 4)")
    raise ValueError(f"Unknown source type: {stype!r}")
