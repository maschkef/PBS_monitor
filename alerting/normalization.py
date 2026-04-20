"""Pure data normalization, migration, and default-state helpers.

No file I/O.  All functions are stateless and have no external dependencies
beyond the Python standard library.  Imported by schedule.py and monitor.py.
"""

import json
from datetime import datetime, timezone
from pathlib import Path


# ── Basic numeric / time converters ──────────────────────────────────────────

def coerce_int(value):
    """Convert a value to int if possible, else return None."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def unix_to_iso(timestamp):
    """Convert a UNIX timestamp to an ISO 8601 string."""
    timestamp = coerce_int(timestamp)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def format_schedule_time(minute_of_day):
    """Format a minute-of-day integer as HH:MM."""
    hours = minute_of_day // 60
    minutes = minute_of_day % 60
    return f"{hours:02d}:{minutes:02d}"


def weekday_name(index):
    """Return a short weekday name for a 0-based weekday index."""
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][index]


# ── Rule / group key helpers ──────────────────────────────────────────────────

def make_rule_key(datastore_id, namespace, backup_type, backup_id):
    """Build a stable JSON-encoded composite key for persisted group rules."""
    return json.dumps([
        datastore_id or "",
        namespace or "",
        backup_type or "",
        str(backup_id or ""),
    ], separators=(",", ":"))


# ── Slot / rule normalization ─────────────────────────────────────────────────

def normalize_weekly_slots(slots):
    """Normalize and sort weekly slot definitions."""
    normalized = []
    seen = set()
    for slot in slots or []:
        if not isinstance(slot, dict):
            continue
        weekday = coerce_int(slot.get("weekday"))
        minute_of_day = coerce_int(slot.get("minute_of_day"))
        if weekday is None or minute_of_day is None:
            continue
        if weekday < 0 or weekday > 6:
            continue
        if minute_of_day < 0 or minute_of_day > ((24 * 60) - 1):
            continue
        key = (weekday, minute_of_day)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "weekday": weekday,
            "weekday_name": weekday_name(weekday),
            "minute_of_day": minute_of_day,
            "time": format_schedule_time(minute_of_day),
        })
    normalized.sort(key=lambda item: (item["weekday"], item["minute_of_day"]))
    return normalized


def normalize_daily_slots(slots):
    """Normalize and sort daily slot definitions."""
    normalized = []
    seen = set()
    for slot in slots or []:
        if not isinstance(slot, dict):
            continue
        minute_of_day = coerce_int(slot.get("minute_of_day"))
        if minute_of_day is None:
            continue
        if minute_of_day < 0 or minute_of_day > ((24 * 60) - 1):
            continue
        if minute_of_day in seen:
            continue
        seen.add(minute_of_day)
        normalized.append({
            "minute_of_day": minute_of_day,
            "time": format_schedule_time(minute_of_day),
        })
    normalized.sort(key=lambda item: item["minute_of_day"])
    return normalized


def normalize_group_rule(raw_rule):
    """Normalize one persisted group rule entry."""
    if not isinstance(raw_rule, dict):
        raw_rule = {}

    schedule_kind = raw_rule.get("schedule_kind")
    if schedule_kind not in {"daily", "weekly", "interval", "none"}:
        schedule_kind = "none"

    interval_minutes = coerce_int(raw_rule.get("interval_minutes"))
    if interval_minutes is not None and interval_minutes <= 0:
        interval_minutes = None

    interval_anchor_minute = coerce_int(raw_rule.get("interval_anchor_minute"))
    if interval_anchor_minute is not None and not (0 <= interval_anchor_minute <= 1439):
        interval_anchor_minute = None

    return {
        "datastore_id": raw_rule.get("datastore_id") or "",
        "namespace": raw_rule.get("namespace") or "",
        "backup_type": raw_rule.get("backup_type") or "",
        "backup_id": str(raw_rule.get("backup_id", "")),
        "display_name": raw_rule.get("display_name"),
        "locked": bool(raw_rule.get("locked", False)),
        "schedule_kind": schedule_kind,
        "timezone": raw_rule.get("timezone") or "local",
        "daily_slots": normalize_daily_slots(raw_rule.get("daily_slots")),
        "weekly_slots": normalize_weekly_slots(raw_rule.get("weekly_slots")),
        "interval_minutes": interval_minutes,
        "interval_anchor_minute": interval_anchor_minute,
        "updated_at": raw_rule.get("updated_at"),
        "updated_by": raw_rule.get("updated_by") or "learning",
    }


def normalize_ignored_group(raw_group):
    """Normalize one ignored backup-group selector."""
    if not isinstance(raw_group, dict):
        return None

    normalized = {
        "datastore_id": None,
        "namespace": None,
        "backup_type": None,
        "backup_id": None,
        "display_name": None,
    }
    for key in ("datastore_id", "namespace", "backup_type", "backup_id"):
        if key not in raw_group:
            continue
        value = raw_group.get(key)
        if value is None:
            continue
        normalized[key] = str(value)
    if all(normalized[k] is None for k in ("datastore_id", "namespace", "backup_type", "backup_id")):
        return None
    if raw_group.get("display_name"):
        normalized["display_name"] = str(raw_group["display_name"])
    return normalized


def normalize_ignored_groups(raw_groups):
    """Normalize the configured ignored backup-group selectors."""
    normalized = []
    for raw_group in raw_groups or []:
        entry = normalize_ignored_group(raw_group)
        if entry is not None:
            normalized.append(entry)
    return normalized


def is_group_ignored(config, datastore_id, namespace, backup_type, backup_id):
    """Return True when a backup group matches an ignore selector."""
    datastore_id = str(datastore_id or "")
    namespace = str(namespace or "")
    backup_type = str(backup_type or "")
    backup_id = str(backup_id or "")

    for ignored_group in config.get("ignored_groups") or []:
        if ignored_group.get("datastore_id") is not None and ignored_group["datastore_id"] != datastore_id:
            continue
        if ignored_group.get("namespace") is not None and ignored_group["namespace"] != namespace:
            continue
        if ignored_group.get("backup_type") is not None and ignored_group["backup_type"] != backup_type:
            continue
        if ignored_group.get("backup_id") is not None and ignored_group["backup_id"] != backup_id:
            continue
        return True
    return False


# ── Default state constructors ────────────────────────────────────────────────

def empty_inventory_summary():
    """Return an empty backup inventory summary."""
    return {
        "namespace_count": 0,
        "group_count": 0,
        "snapshot_count": 0,
    }


def default_datastore_state(name="unknown"):
    """Return default persistent state for one datastore."""
    return {
        "name": name,
        "last_inventory_at": None,
        "inventory_summary": empty_inventory_summary(),
        "schedule_summary": {
            "learned_group_count": 0,
            "active_slot_count": 0,
        },
        "backup_groups": {},
    }


def default_state():
    """Return the default persisted state document."""
    return {
        "version": _STATE_VERSION,
        "datastores": {},
        "last_alerts": {},
    }


def default_group_rules():
    """Return the default persisted group-rule document."""
    return {
        "version": _GROUP_RULES_VERSION,
        "groups": {},
    }


# Version constants (must match the values kept in monitor.py for I/O functions).
_STATE_VERSION = 2
_GROUP_RULES_VERSION = 1


# ── Snapshot history helpers ──────────────────────────────────────────────────

def normalize_snapshot_entries(entries, limit=None):
    """Normalize and deduplicate persisted snapshot entries."""
    normalized = {}
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        backup_time = coerce_int(entry.get("backup_time"))
        if backup_time is None:
            continue
        normalized[backup_time] = {
            "backup_time": backup_time,
            "size": coerce_int(entry.get("size")),
            "protected": bool(entry.get("protected", False)),
            "comment": entry.get("comment"),
        }
    snapshots = sorted(normalized.values(), key=lambda item: item["backup_time"], reverse=True)
    if limit is not None:
        snapshots = snapshots[:limit]
    return snapshots


def merge_snapshot_histories(existing_entries, new_entries, limit):
    """Merge snapshot history by backup_time, keeping the newest entries."""
    merged = {}
    for entry in normalize_snapshot_entries(existing_entries):
        merged[entry["backup_time"]] = entry
    for entry in normalize_snapshot_entries(new_entries):
        merged[entry["backup_time"]] = entry
    return normalize_snapshot_entries(merged.values(), limit)


# ── State / rule migration ────────────────────────────────────────────────────

def migrate_inventory_summary(summary):
    """Normalize an inventory summary loaded from state."""
    if not isinstance(summary, dict):
        return empty_inventory_summary()

    normalized = empty_inventory_summary()
    for key in normalized:
        value = coerce_int(summary.get(key))
        normalized[key] = value if value is not None else 0
    return normalized


def migrate_backup_group_state(raw_group):
    """Normalize a persisted backup-group state entry."""
    if not isinstance(raw_group, dict):
        raw_group = {}

    backup_type = raw_group.get("backup_type") or "unknown"
    backup_id = str(raw_group.get("backup_id", ""))
    current_snapshots = normalize_snapshot_entries(
        raw_group.get("current_snapshots") or raw_group.get("recent_snapshots"),
        _MAX_CURRENT_SNAPSHOT_DETAILS,
    )
    observed_snapshots = normalize_snapshot_entries(
        raw_group.get("observed_snapshots") or current_snapshots,
        _MAX_OBSERVED_SNAPSHOT_HISTORY,
    )

    current_snapshot_count = coerce_int(raw_group.get("current_snapshot_count"))
    if current_snapshot_count is None:
        current_snapshot_count = coerce_int(raw_group.get("backup_count"))
    if current_snapshot_count is None:
        current_snapshot_count = len(current_snapshots)

    protected_snapshot_count = coerce_int(raw_group.get("protected_snapshot_count"))
    if protected_snapshot_count is None:
        protected_snapshot_count = sum(1 for entry in current_snapshots if entry["protected"])

    schedule_model = raw_group.get("schedule_model")
    if not isinstance(schedule_model, dict):
        schedule_model = {
            "status": "learning",
            "timezone": "local",
            "evaluated_at": None,
            "slot_count": 0,
            "active_slot_count": 0,
            "slots": [],
        }

    learned_schedule_model = raw_group.get("learned_schedule_model")
    if not isinstance(learned_schedule_model, dict):
        learned_schedule_model = None

    return {
        "namespace": raw_group.get("namespace") or "",
        "backup_type": backup_type,
        "backup_id": backup_id,
        "display_name": raw_group.get("display_name") or raw_group.get("comment") or f"{backup_type}/{backup_id}",
        "comment": raw_group.get("comment"),
        "first_observed_at": raw_group.get("first_observed_at"),
        "last_observed_at": raw_group.get("last_observed_at"),
        "missing_since": raw_group.get("missing_since"),
        "last_backup_at": raw_group.get("last_backup_at") or raw_group.get("last_backup"),
        "current_snapshot_count": current_snapshot_count,
        "protected_snapshot_count": protected_snapshot_count,
        "current_snapshots": current_snapshots,
        "observed_snapshots": observed_snapshots,
        "learned_schedule_model": learned_schedule_model,
        "schedule_model": schedule_model,
    }


def migrate_state(raw_state):
    """Migrate older persisted state into the current schema."""
    state = {
        "version": _STATE_VERSION,
        "datastores": {},
        "last_alerts": {},
    }
    if not isinstance(raw_state, dict):
        return state

    last_alerts = raw_state.get("last_alerts")
    if isinstance(last_alerts, dict):
        state["last_alerts"] = last_alerts

    raw_datastores = raw_state.get("datastores")
    if not isinstance(raw_datastores, dict):
        return state

    for ds_id, raw_ds_state in raw_datastores.items():
        ds_name = ds_id
        if isinstance(raw_ds_state, dict):
            ds_name = raw_ds_state.get("name") or ds_id

        migrated_ds_state = default_datastore_state(ds_name)
        if isinstance(raw_ds_state, dict):
            migrated_ds_state["last_inventory_at"] = raw_ds_state.get("last_inventory_at")
            migrated_ds_state["inventory_summary"] = migrate_inventory_summary(
                raw_ds_state.get("inventory_summary")
            )
            if isinstance(raw_ds_state.get("schedule_summary"), dict):
                migrated_ds_state["schedule_summary"] = {
                    "learned_group_count": coerce_int(
                        raw_ds_state["schedule_summary"].get("learned_group_count")
                    ) or 0,
                    "active_slot_count": coerce_int(
                        raw_ds_state["schedule_summary"].get("active_slot_count")
                    ) or 0,
                }
            raw_groups = raw_ds_state.get("backup_groups")
            if isinstance(raw_groups, dict):
                migrated_ds_state["backup_groups"] = {
                    str(group_key): migrate_backup_group_state(group_state)
                    for group_key, group_state in raw_groups.items()
                }

        state["datastores"][str(ds_id)] = migrated_ds_state

    return state


def migrate_group_rules(raw_rules):
    """Normalize persisted group rules."""
    rules = {
        "version": _GROUP_RULES_VERSION,
        "groups": {},
    }
    if not isinstance(raw_rules, dict):
        return rules

    raw_groups = raw_rules.get("groups")
    if not isinstance(raw_groups, dict):
        return rules

    rules["groups"] = {
        str(rule_key): normalize_group_rule(rule)
        for rule_key, rule in raw_groups.items()
    }
    return rules


# Module-level capacity constants used by migrate_backup_group_state.
# These are set to match monitor.py's MAX_* constants and must stay in sync.
_MAX_CURRENT_SNAPSHOT_DETAILS = 24
_MAX_OBSERVED_SNAPSHOT_HISTORY = 1000
