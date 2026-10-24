"""Factory and validator for building SourceAdapter instances from config.

Phase 3 introduces `validate_source_entries` so `load_config()` can normalise a
`sources: []` config list (synthesising a default remote-backups entry when
empty) and validate every entry's identity/type before any adapter is built.

Phase 4 wires in a real PBS adapter: the validator now enforces the PBS
schema (base_url, node, token_env, optional fingerprint/verify_tls/
datastores/label) and `build_source` constructs a `PbsAdapter`.
"""

from __future__ import annotations

import re

from .base import SourceAdapter
from .remote_backups import RemoteBackupsAdapter

_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_VALID_TYPES = frozenset({"remote_backups", "pbs"})
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
            api_key_env = normalised.setdefault("api_key_env", "API_KEY")
            if not _ENV_VAR_NAME_RE.match(api_key_env):
                raise ValueError(
                    f"sources[{i}].api_key_env must be a valid environment "
                    f"variable name (got {api_key_env!r})"
                )
        elif stype == "pbs":
            base_url = entry.get("base_url")
            if not isinstance(base_url, str) or not base_url:
                raise ValueError(
                    f"sources[{i}].base_url must be a non-empty string "
                    f"for type 'pbs'"
                )
            if not base_url.startswith(("http://", "https://")):
                raise ValueError(
                    f"sources[{i}].base_url must start with http:// or https://"
                )

            raw_node = entry.get("node")
            if raw_node is None:
                node = "localhost"
            elif not isinstance(raw_node, str):
                raise ValueError(
                    f"sources[{i}].node must be a non-empty string "
                    f"for type 'pbs'"
                )
            else:
                node = raw_node.strip() or "localhost"
            if "." in node or "://" in node or "/" in node:
                raise ValueError(
                    f"sources[{i}].node must be a short hostname, not an "
                    f"FQDN or URL (got {node!r})"
                )

            token_env = entry.get("token_env")
            if not isinstance(token_env, str) or not token_env:
                raise ValueError(
                    f"sources[{i}].token_env must be a non-empty string "
                    f"for type 'pbs'"
                )
            if not _ENV_VAR_NAME_RE.match(token_env):
                raise ValueError(
                    f"sources[{i}].token_env must be a valid environment "
                    f"variable name (got {token_env!r})"
                )

            fp = entry.get("fingerprint")
            if fp is not None and (not isinstance(fp, str) or not fp):
                raise ValueError(
                    f"sources[{i}].fingerprint must be a non-empty string "
                    f"if set"
                )
            verify_tls = entry.get("verify_tls", True)
            if not isinstance(verify_tls, bool):
                raise ValueError(
                    f"sources[{i}].verify_tls must be a boolean"
                )
            if not verify_tls and not fp:
                raise ValueError(
                    f"sources[{i}]: verify_tls=false requires "
                    f"fingerprint pinning"
                )
            datastores = entry.get("datastores", [])
            if not isinstance(datastores, list) or any(
                not isinstance(x, str) or not x for x in datastores
            ):
                raise ValueError(
                    f"sources[{i}].datastores must be a list of "
                    f"non-empty strings"
                )
            label = entry.get("label")
            if label is not None and not isinstance(label, str):
                raise ValueError(
                    f"sources[{i}].label must be a string if set"
                )
            normalised.setdefault("verify_tls", True)
            normalised.setdefault("datastores", [])
            normalised["node"] = node
            normalised["token_env"] = token_env

        validated.append(normalised)
    return validated


def build_source(cfg_entry: dict) -> SourceAdapter:
    """Build a SourceAdapter from a `sources: []` config entry.

    Accepted shapes:
        {"id": "<slug>", "type": "remote_backups",
         "api_base": "<url>", "api_key_env": "API_KEY"}
        {"id": "<slug>", "type": "pbs",
         "base_url": "<url>", "node": "<name>", "token_env": "<ENV>",
         "label": "<optional>", "fingerprint": "<optional>",
         "verify_tls": True, "datastores": []}
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
        from .pbs import PbsAdapter
        return PbsAdapter(
            source_id=source_id,
            base_url=cfg_entry["base_url"],
            node=cfg_entry["node"],
            token_env=cfg_entry["token_env"],
            label=cfg_entry.get("label"),
            fingerprint=cfg_entry.get("fingerprint"),
            verify_tls=cfg_entry.get("verify_tls", True),
            datastores=cfg_entry.get("datastores") or None,
        )
    raise ValueError(f"Unknown source type: {stype!r}")
