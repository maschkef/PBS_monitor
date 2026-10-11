"""Status-feed enrichment helpers.

Fetches and parses the JSON status feed at
`https://status.remote-backups.com/index.json` and provides a small helper
that enriches an ``Alert`` with context about currently-open incidents.

Design goals
------------
- No imports from :mod:`alerting.monitor` — avoid cycles.
- All network/parse failures degrade silently to ``None``; enrichment must
  never break the alert path.
- A single module-level cache keeps repeated calls within one ``run_check``
  cheap; the webui holds its own cache.
"""

from __future__ import annotations

import sys
import time
from datetime import UTC, datetime, timedelta

import requests

# ── Allowlist ────────────────────────────────────────────────────────────────
#
# Only alerts whose title appears here are enriched.  The remaining alert
# types (storage/GC/verification/replication) have nothing to do with
# platform-side availability.

GLOBAL_ALERT_TITLES: frozenset[str] = frozenset({
    "API Unreachable",
    "API Unhealthy",
    "Monitoring API Error",
})

DATASTORE_ALERT_TITLES: frozenset[str] = frozenset({
    "Missed Backup Window",
    "Missed Backup Interval",
    "Snapshots Unexpectedly Removed",
    "All Backups Gone",
    "Host Offline",
})

ENRICHABLE_TITLES: frozenset[str] = GLOBAL_ALERT_TITLES | DATASTORE_ALERT_TITLES

DEFAULT_FEED_URL = "https://status.remote-backups.com/index.json"

_FEED_CACHE: dict = {"ts": 0.0, "data": None}


# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_status_feed(
    url: str = DEFAULT_FEED_URL,
    *,
    timeout: int = 10,
    cache: dict | None = None,
    cache_ttl_seconds: int = 600,
    force_refresh: bool = False,
) -> dict | None:
    """Return the parsed JSON feed or ``None`` on any error.

    Uses an in-memory TTL cache; the default module-level cache is shared
    across callers unless an explicit ``cache`` dict is supplied.
    """
    if cache is None:
        cache = _FEED_CACHE
    now = time.monotonic()
    if (not force_refresh
            and cache.get("data") is not None
            and (now - cache.get("ts", 0.0)) < cache_ttl_seconds):
        return cache["data"]

    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        feed = resp.json()
    except requests.RequestException as exc:
        print(f"[status_feed] fetch failed: {exc}", file=sys.stderr)
        return None
    except ValueError as exc:  # json decode
        print(f"[status_feed] parse failed: {exc}", file=sys.stderr)
        return None

    cache["ts"] = now
    cache["data"] = feed
    return feed


# ── Indexing ─────────────────────────────────────────────────────────────────

def index_resources(feed: dict) -> tuple[dict[str, str], dict[str, str]]:
    """Build ``(public_name_lower -> resource_id, resource_id -> public_name)``.

    Resource ids in the feed are strings (JSON:API convention).  Public
    names are compared case-insensitively for the forward map.
    """
    name_to_id: dict[str, str] = {}
    id_to_name: dict[str, str] = {}
    for entry in feed.get("included", []) or []:
        if entry.get("type") != "status_page_resource":
            continue
        rid = str(entry.get("id") or "")
        attrs = entry.get("attributes") or {}
        public_name = (attrs.get("public_name") or "").strip()
        if not rid or not public_name:
            continue
        id_to_name[rid] = public_name
        name_to_id[public_name.lower()] = rid
    return name_to_id, id_to_name


def _iter_status_reports(feed: dict):
    for entry in feed.get("included", []) or []:
        if entry.get("type") == "status_report":
            yield entry


def parse_iso(value) -> datetime | None:
    """Parse an ISO-8601 string; returns ``None`` on falsy or malformed input."""
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# ── Matching ─────────────────────────────────────────────────────────────────

def host_matches_public_name(host: str, public_name: str) -> bool:
    """Return True when a configured host matches a status-page resource name.

    Rule: case-insensitive, trimmed. Match on exact equality OR when the
    public name begins with ``"<host> - "`` — this handles multi-instance
    resources published as e.g. ``"fra1-ingress.pbs-host.de - Instance 1"``
    while a single base hostname is configured.
    """
    h = (host or "").strip().lower()
    pn = (public_name or "").strip().lower()
    if not h or not pn:
        return False
    return pn == h or pn.startswith(h + " - ")


def _resolve_target_rids(host: str, name_to_id: dict[str, str]) -> set[str]:
    """Return every resource ID whose public_name matches ``host``."""
    h = (host or "").strip().lower()
    if not h:
        return set()
    return {rid for pn_lower, rid in name_to_id.items()
            if pn_lower == h or pn_lower.startswith(h + " - ")}


def find_matching_incidents(
    feed: dict,
    *,
    host: str | None = None,
    at_time: datetime | None = None,
    lookback_minutes: int = 120,
    max_results: int = 10,
) -> list[dict]:
    """Return a list of incidents matching the given host and/or time.

    - If ``host`` is given: incidents where that host (matched via
      ``public_name`` against ``affected_resources``) is present AND the
      incident overlaps the time window ``[at_time - lookback, at_time]``
      (ongoing incidents are always kept). The host match handles
      multi-instance resources like ``"<host> - Instance 1"``.
    - If ``host`` is ``None``: only ongoing incidents (any host) are returned
      — used for API-wide alerts.

    Sort: ongoing first, then by ``starts_at`` descending.
    """
    if not isinstance(feed, dict):
        return []

    name_to_id, id_to_name = index_resources(feed)
    target_rids: set[str] | None = None
    if host:
        target_rids = _resolve_target_rids(host, name_to_id)
        if not target_rids:
            # host configured but unknown to the status page — treat like
            # "no match" (don't silently fall back to all-ongoing, that
            # would be misleading).
            return []

    if at_time is None:
        at_time = datetime.now(UTC)
    elif at_time.tzinfo is None:
        at_time = at_time.replace(tzinfo=UTC)
    lookback_delta = timedelta(minutes=max(lookback_minutes, 0))

    matches: list[dict] = []
    for report in _iter_status_reports(feed):
        attrs = report.get("attributes") or {}
        starts_at = parse_iso(attrs.get("starts_at"))
        ends_at = parse_iso(attrs.get("ends_at"))
        agg = (attrs.get("aggregate_state") or "").lower()
        # Treat anything whose aggregate_state is not "resolved" and whose
        # ends_at is either missing or in the future as "ongoing".
        is_ongoing = (
            agg != "resolved"
            and (ends_at is None or ends_at > at_time)
        )

        affected = attrs.get("affected_resources") or []
        affected_ids = [str(a.get("status_page_resource_id") or "") for a in affected]
        affected_hosts = [id_to_name[rid] for rid in affected_ids if rid in id_to_name]

        matched_name: str | None = None
        if target_rids is not None:
            matched_affected = [rid for rid in affected_ids if rid in target_rids]
            if not matched_affected:
                continue
            matched_name = id_to_name.get(matched_affected[0])
            # Host-scoped matching: ongoing OR overlapping the lookback window.
            if not is_ongoing:
                if starts_at is None and ends_at is None:
                    continue
                # Keep if the incident was active at any point in
                # [at_time - lookback, at_time].
                window_lo = at_time - lookback_delta
                incident_end = ends_at or at_time
                incident_start = starts_at or incident_end
                if incident_end < window_lo or incident_start > at_time:
                    continue
        elif not is_ongoing:
            # Global scope: only ongoing incidents are relevant.
            continue

        matches.append({
            "id": report.get("id"),
            "title": (attrs.get("title") or "").strip(),
            "report_type": (attrs.get("report_type") or "").strip(),
            "aggregate_state": agg,
            "starts_at": starts_at,
            "ends_at": ends_at,
            "is_ongoing": is_ongoing,
            "affected_hosts": affected_hosts,
            "matched_host": matched_name,
        })

    def _sort_key(item):
        start = item["starts_at"]
        return (
            0 if item["is_ongoing"] else 1,
            -(start.timestamp() if start else 0.0),
        )

    matches.sort(key=_sort_key)
    return matches[:max_results]


# ── Formatting ───────────────────────────────────────────────────────────────

def _format_dt(dt: datetime | None) -> str:
    if dt is None:
        return "?"
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def format_enrichment_text(incidents: list[dict], *, max_items: int = 3) -> str:
    """Render a short enrichment paragraph; return ``""`` when empty."""
    if not incidents:
        return ""
    lines = ["— Possibly related platform incident(s):"]
    for item in incidents[:max_items]:
        state = "ONGOING" if item["is_ongoing"] else "RESOLVED"
        kind = item.get("report_type") or "incident"
        start = _format_dt(item.get("starts_at"))
        end = _format_dt(item.get("ends_at")) if item.get("ends_at") else "ongoing"
        title = item.get("title") or "(no title)"
        affected = item.get("affected_hosts") or []
        host_hint = ""
        if item.get("matched_host"):
            host_hint = f" [match: {item['matched_host']}]"
        elif affected:
            sample = ", ".join(affected[:3])
            if len(affected) > 3:
                sample += f" (+{len(affected) - 3})"
            host_hint = f" [hosts: {sample}]"
        lines.append(
            f"  • [{state}] {title} ({kind}, {start} → {end}){host_hint}"
        )
    if len(incidents) > max_items:
        lines.append(f"  … and {len(incidents) - max_items} more.")
    return "\n".join(lines)


# ── Host resolution ──────────────────────────────────────────────────────────

def resolve_host_for_alert(
    config: dict,
    *,
    datastore_id: str | None = None,
    namespace: str | None = None,
) -> str | None:
    """Resolve the configured host for a datastore+namespace pair.

    Priority: (datastore_id, namespace) > (datastore_id, "") > default_host.
    Empty strings are normalized to ``None`` / "".
    """
    sf = (config or {}).get("status_feed") or {}
    overrides = (config or {}).get("host_overrides") or []
    ds_id = (datastore_id or "").strip() or None
    ns = (namespace or "").strip()

    exact_match: str | None = None
    ds_default: str | None = None
    for entry in overrides:
        if not isinstance(entry, dict):
            continue
        if (entry.get("datastore_id") or "").strip() != ds_id:
            continue
        host = (entry.get("host") or "").strip()
        if not host:
            continue
        entry_ns = (entry.get("namespace") or "").strip()
        if ds_id and ns and entry_ns == ns:
            exact_match = host
            break  # most-specific match wins
        if ds_id and entry_ns == "":
            ds_default = host

    if exact_match:
        return exact_match
    if ds_default:
        return ds_default
    default_host = (sf.get("default_host") or "").strip()
    return default_host or None


# ── Enrichment entry point ───────────────────────────────────────────────────

def enrich_alert(
    alert,
    config: dict,
    *,
    event_time: datetime | None = None,
    cache: dict | None = None,
) -> None:
    """Append status-feed context to ``alert.message`` for enrichable titles.

    Mutates ``alert`` in place.  Any exception is swallowed and logged — this
    helper must never raise into the alert path.
    """
    try:
        sf = (config or {}).get("status_feed") or {}
        if not sf.get("enabled", True):
            return
        title = getattr(alert, "title", "") or ""
        if title not in ENRICHABLE_TITLES:
            return

        url = (sf.get("url") or "").strip() or DEFAULT_FEED_URL
        cache_minutes = sf.get("cache_minutes")
        try:
            cache_ttl = int(cache_minutes) * 60 if cache_minutes is not None else 600
        except (TypeError, ValueError):
            cache_ttl = 600
        if cache_ttl < 0:
            cache_ttl = 600

        lookback = sf.get("lookback_minutes")
        try:
            lookback_minutes = int(lookback) if lookback is not None else 120
        except (TypeError, ValueError):
            lookback_minutes = 120

        if title in GLOBAL_ALERT_TITLES:
            host = None
        else:
            host = resolve_host_for_alert(
                config,
                datastore_id=getattr(alert, "datastore_id", None),
                namespace=getattr(alert, "namespace", None),
            )
            if not host:
                # No host configured → cannot match; skip quietly.
                return

        feed = fetch_status_feed(
            url, cache=cache, cache_ttl_seconds=cache_ttl,
        )
        if feed is None:
            return

        incidents = find_matching_incidents(
            feed,
            host=host,
            at_time=event_time or datetime.now(UTC),
            lookback_minutes=lookback_minutes,
        )
        text = format_enrichment_text(incidents)
        if not text:
            return

        current = getattr(alert, "message", "") or ""
        alert.message = f"{current}\n\n{text}" if current else text
        tags = getattr(alert, "tags", None)
        if isinstance(tags, list) and "status-feed" not in tags:
            tags.append("status-feed")
    except Exception as exc:  # noqa: BLE001 — never raise into alert path
        print(f"[status_feed] enrich_alert failed: {exc}", file=sys.stderr)
