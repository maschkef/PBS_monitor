#!/usr/bin/env python3
"""
PBS Monitor — Alerting script for remote-backups.com datastores.
Checks datastore health and sends alerts via ntfy on problems.

Tracks PBS backup inventory per namespace and backup group so future alerting
can learn schedules from real snapshot history instead of aggregate counts.
State is persisted to a JSON file between runs.

Usage:
    python monitor.py                  # single check
    python monitor.py --daemon 1800    # check every 1800 seconds (30 minutes)

Cron example (every 5 minutes):
    */5 * * * * cd /path/to/alerting && python monitor.py
"""

import argparse
import json
import shutil
import os
import signal
import statistics
import sys
import time
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from dotenv import load_dotenv

# ─── Configuration ───────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
ENV_PATH = SCRIPT_DIR.parent / ".env"

# Support a configurable data directory (e.g. for Docker deployments where
# config/state must be stored in a mounted volume separate from the code).
# Defaults to the script directory for non-Docker use.
_data_dir_env = os.environ.get("ALERTING_DATA_DIR", "").strip()
DATA_DIR = Path(_data_dir_env) if _data_dir_env else SCRIPT_DIR

CONFIG_PATH = DATA_DIR / "config.json"
STATE_PATH = DATA_DIR / "state.json"

load_dotenv(ENV_PATH)

STATE_VERSION = 2
MAX_CURRENT_SNAPSHOT_DETAILS = 24
MAX_OBSERVED_SNAPSHOT_HISTORY = 1000
GROUP_RULES_VERSION = 1
GROUP_RULES_PATH = DATA_DIR / "group_rules.json"
INTERVAL_MODEL_MAX_MINUTES = 360

DEFAULT_CONFIG = {
    "api_base": "https://api.remote-backups.com",
    # "ntfy_url": "https://ntfy.sh",  # Example: configure this to enable push notifications
    "ntfy_topic": "",  # REQUIRED: set this to enable push notifications (e.g., "your-pbs-alerts")
    "ntfy_token": "",
    "ignored_groups": [],
    "thresholds": {
        "storage_warn_percent": 80,
        "storage_crit_percent": 90,
        "gc_max_age_hours": 36,
        "verification_max_age_days": 14,
    },
    "quiet_hours": {
        "enabled": False,
        "start": "22:00",
        "end": "07:00",
        "min_priority": 4,
    },
    "schedule_learning": {
        "enabled": True,
        "timezone": "local",
        "history_window_days": 60,
        "min_occurrences": 2,
        "time_tolerance_minutes": 30,
        "due_grace_minutes": 30,
        "stale_after_days": 8,
    },
    "alert_cooldown_minutes": 60,
    "daemon_interval_seconds": 1800,  # 30 minutes default for daemon mode
}


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
        "version": STATE_VERSION,
        "datastores": {},
        "last_alerts": {},
    }


def default_group_rules():
    """Return the default persisted group-rule document."""
    return {
        "version": GROUP_RULES_VERSION,
        "groups": {},
    }


def make_rule_key(datastore_id, namespace, backup_type, backup_id):
    """Build a stable key for persisted group rules."""
    return json.dumps([
        datastore_id or "",
        namespace or "",
        backup_type or "",
        str(backup_id or ""),
    ], separators=(",", ":"))


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
    }
    for key in normalized:
        if key not in raw_group:
            continue
        value = raw_group.get(key)
        if value is None:
            continue
        normalized[key] = str(value)
    if all(value is None for value in normalized.values()):
        return None
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


def migrate_group_rules(raw_rules):
    """Normalize persisted group rules."""
    rules = default_group_rules()
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


def load_group_rules():
    """Load persisted group rules."""
    if GROUP_RULES_PATH.exists():
        with open(GROUP_RULES_PATH) as f:
            return migrate_group_rules(json.load(f))
    return default_group_rules()


def save_group_rules(group_rules):
    """Persist group rules to disk."""
    group_rules["version"] = GROUP_RULES_VERSION
    with open(GROUP_RULES_PATH, "w") as f:
        json.dump(group_rules, f, indent=2)


def coerce_int(value):
    """Convert a value to int if possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def unix_to_iso(timestamp):
    """Convert a UNIX timestamp to an ISO string."""
    timestamp = coerce_int(timestamp)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


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
    """Merge snapshot history by backup_time and keep the newest entries."""
    merged = {}
    for entry in normalize_snapshot_entries(existing_entries):
        merged[entry["backup_time"]] = entry
    for entry in normalize_snapshot_entries(new_entries):
        merged[entry["backup_time"]] = entry
    return normalize_snapshot_entries(merged.values(), limit)


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
        MAX_CURRENT_SNAPSHOT_DETAILS,
    )
    observed_snapshots = normalize_snapshot_entries(
        raw_group.get("observed_snapshots") or current_snapshots,
        MAX_OBSERVED_SNAPSHOT_HISTORY,
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
    state = default_state()
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


def load_config():
    """Load config, create from example if missing."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        # Merge with defaults for new keys
        merged = {**DEFAULT_CONFIG, **cfg}
        merged["thresholds"] = {**DEFAULT_CONFIG["thresholds"], **cfg.get("thresholds", {})}
        merged["quiet_hours"] = {**DEFAULT_CONFIG["quiet_hours"], **cfg.get("quiet_hours", {})}
        merged["schedule_learning"] = {
            **DEFAULT_CONFIG["schedule_learning"],
            **cfg.get("schedule_learning", {}),
        }
        merged["ignored_groups"] = normalize_ignored_groups(cfg.get("ignored_groups"))
        return merged

    example_path = Path(__file__).parent / "config.json.example"
    if example_path.exists():
        shutil.copy(example_path, CONFIG_PATH)
        print(f"Copied config from {example_path} to {CONFIG_PATH}")
    else:
        with open(CONFIG_PATH, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        print(f"Created default config at {CONFIG_PATH}")
            
    print("Edit ntfy_topic (and optionally ntfy_token) before running.")
    return DEFAULT_CONFIG


def load_state():
    """Load persistent state and migrate it to the current schema."""
    if STATE_PATH.exists():
        with open(STATE_PATH) as f:
            return migrate_state(json.load(f))
    return default_state()


def save_state(state):
    """Persist state to disk."""
    state["version"] = STATE_VERSION
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


# ─── API Client ──────────────────────────────────────────────────────────────

def api_get(config, path, params=None):
    """Authenticated GET request to monitoring API."""
    api_key = os.environ.get("API_KEY", "")
    if not api_key:
        raise RuntimeError("API_KEY not set in environment or .env")
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = requests.get(
        f"{config['api_base']}{path}",
        headers=headers,
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_datastores(config):
    """Fetch all datastores with details."""
    datastores = api_get(config, "/monitoring/v1/datastores")
    result = []
    for ds in datastores:
        try:
            detail = api_get(config, f"/monitoring/v1/datastores/{ds['id']}")
            result.append(detail)
        except requests.RequestException:
            result.append(ds)
    return result


def make_group_key(namespace, backup_type, backup_id):
    """Build a stable key for one backup group."""
    return json.dumps([
        namespace or "",
        backup_type or "",
        str(backup_id or ""),
    ], separators=(",", ":"))


def build_snapshot_record(snapshot):
    """Normalize one snapshot from the live API."""
    backup_time = coerce_int(snapshot.get("backup_time"))
    if backup_time is None:
        return None

    return {
        "backup_time": backup_time,
        "size": coerce_int(snapshot.get("size")),
        "protected": bool(snapshot.get("protected", False)),
        "comment": snapshot.get("comment"),
    }


def build_backup_group_record(namespace, group, group_snapshots):
    """Build one normalized backup-group record from API data."""
    backup_type = group.get("backup_type")
    backup_id = str(group.get("backup_id", ""))
    last_backup = coerce_int(group.get("last_backup"))
    if last_backup is None and group_snapshots:
        last_backup = group_snapshots[0]["backup_time"]

    backup_count = coerce_int(group.get("backup_count"))
    if backup_count is None:
        backup_count = len(group_snapshots)

    latest_comment = next(
        (entry.get("comment") for entry in group_snapshots if entry.get("comment")),
        None,
    )

    return {
        "group_key": make_group_key(namespace, backup_type, backup_id),
        "namespace": namespace or "",
        "backup_type": backup_type,
        "backup_id": backup_id,
        "display_name": latest_comment or group.get("comment") or f"{backup_type}/{backup_id}",
        "comment": group.get("comment"),
        "last_backup_at": unix_to_iso(last_backup),
        "backup_count": backup_count,
        "protected_snapshot_count": sum(1 for entry in group_snapshots if entry["protected"]),
        "current_snapshots": normalize_snapshot_entries(group_snapshots, MAX_CURRENT_SNAPSHOT_DETAILS),
    }


def extract_namespace_backup_groups(namespace, namespace_data):
    """Group snapshots by PBS backup group within one namespace."""
    snapshots_by_group = {}
    for snapshot in namespace_data.get("snapshots") or []:
        backup_type = snapshot.get("backup_type")
        backup_id = str(snapshot.get("backup_id", ""))
        if not backup_type:
            continue

        normalized_snapshot = build_snapshot_record(snapshot)
        if not normalized_snapshot:
            continue

        snapshots_by_group.setdefault((backup_type, backup_id), []).append(normalized_snapshot)

    groups = []
    for group in namespace_data.get("groups") or []:
        backup_type = group.get("backup_type")
        backup_id = str(group.get("backup_id", ""))
        if not backup_type:
            continue

        group_snapshots = snapshots_by_group.pop((backup_type, backup_id), [])
        groups.append(build_backup_group_record(namespace, group, group_snapshots))

    for (backup_type, backup_id), group_snapshots in snapshots_by_group.items():
        synthetic_group = {
            "backup_type": backup_type,
            "backup_id": backup_id,
            "last_backup": group_snapshots[0]["backup_time"] if group_snapshots else None,
            "backup_count": len(group_snapshots),
            "comment": None,
        }
        groups.append(build_backup_group_record(namespace, synthetic_group, group_snapshots))

    return groups


def fetch_backup_inventory(config, datastore_id):
    """Fetch full PBS backup inventory for a datastore, grouped by namespace."""
    base_path = f"/monitoring/v1/datastores/{datastore_id}/backups"
    overview = api_get(config, base_path)
    namespace_entries = overview.get("namespaces") or [{"ns": "", "comment": None}]
    namespace_entries = sorted(
        namespace_entries,
        key=lambda item: (item.get("ns", "") != "", item.get("ns", "")),
    )

    groups = []
    for namespace_meta in namespace_entries:
        namespace_value = namespace_meta.get("ns", "")
        namespace_data = api_get(config, base_path, params={"ns": namespace_value})
        groups.extend(extract_namespace_backup_groups(namespace_value, namespace_data))

    snapshot_count = sum(group["backup_count"] for group in groups)
    return {
        "summary": {
            "namespace_count": len(namespace_entries),
            "group_count": len(groups),
            "snapshot_count": snapshot_count,
        },
        "groups": groups,
    }


# ─── Alert Logic ─────────────────────────────────────────────────────────────

class Alert:
    """Represents a single alert."""
    # Priority levels: 1=min, 2=low, 3=default, 4=high, 5=urgent
    def __init__(self, datastore_name, title, message, priority=3, tags=None, key=None, scope="datastore", group_rule_key=None):
        self.datastore_name = datastore_name
        self.title = title
        self.message = message
        self.priority = priority
        self.tags = tags or []
        self.key = key or f"{datastore_name}:{title}"
        self.scope = scope
        self.group_rule_key = group_rule_key


def parse_iso(iso_str):
    """Parse ISO timestamp to datetime."""
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def get_schedule_timezone(config):
    """Return the timezone used for schedule learning."""
    timezone_name = (config.get("schedule_learning") or {}).get("timezone", "local")
    if not timezone_name or timezone_name == "local":
        return datetime.now().astimezone().tzinfo

    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        print(f"[WARN] Unknown schedule timezone '{timezone_name}', using local timezone.")
        return datetime.now().astimezone().tzinfo


def format_schedule_time(minute_of_day):
    """Format a minute-of-day integer as HH:MM."""
    hours = minute_of_day // 60
    minutes = minute_of_day % 60
    return f"{hours:02d}:{minutes:02d}"


def weekday_name(index):
    """Return a short weekday name."""
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][index]


def ensure_group_rule(group_rules, datastore_id, group_state):
    """Return the normalized persisted rule entry for one backup group."""
    rule_key = make_rule_key(
        datastore_id,
        group_state.get("namespace"),
        group_state.get("backup_type"),
        group_state.get("backup_id"),
    )
    groups = group_rules.setdefault("groups", {})
    rule = normalize_group_rule(groups.get(rule_key))
    rule["datastore_id"] = datastore_id
    rule["namespace"] = group_state.get("namespace") or ""
    rule["backup_type"] = group_state.get("backup_type") or ""
    rule["backup_id"] = str(group_state.get("backup_id", ""))
    rule["display_name"] = group_state.get("display_name") or rule.get("display_name")
    groups[rule_key] = rule
    return rule_key, rule


def build_schedule_model_from_rule(rule, fallback_timezone):
    """Build an effective schedule model from a persisted group rule."""
    timezone_name = rule.get("timezone") or fallback_timezone
    if rule.get("schedule_kind") == "interval" and rule.get("interval_minutes"):
        return {
            "kind": "interval",
            "status": "locked" if rule.get("locked") else "configured",
            "timezone": timezone_name,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "slot_count": 1,
            "active_slot_count": 1,
            "interval_minutes": rule["interval_minutes"],
            "interval_human": format_interval_minutes(rule["interval_minutes"]),
            "last_observed_at": None,
            "sample_count": 0,
            "slots": [],
        }

    daily_slots = normalize_daily_slots(rule.get("daily_slots"))
    if rule.get("schedule_kind") == "daily" and daily_slots:
        return {
            "kind": "daily",
            "status": "locked" if rule.get("locked") else "configured",
            "timezone": timezone_name,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "slot_count": len(daily_slots),
            "active_slot_count": len(daily_slots),
            "interval_minutes": None,
            "interval_human": None,
            "slots": [
                {
                    "slot_key": f"daily:{slot['minute_of_day']}",
                    **slot,
                    "sample_count": 0,
                    "first_observed_at": None,
                    "last_observed_at": None,
                    "status": "active",
                }
                for slot in daily_slots
            ],
        }

    weekly_slots = normalize_weekly_slots(rule.get("weekly_slots"))
    if rule.get("schedule_kind") == "weekly" and weekly_slots:
        return {
            "kind": "weekly",
            "status": "locked" if rule.get("locked") else "configured",
            "timezone": timezone_name,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "slot_count": len(weekly_slots),
            "active_slot_count": len(weekly_slots),
            "slots": [
                {
                    "slot_key": f"{slot['weekday']}:{slot['minute_of_day']}",
                    **slot,
                    "sample_count": 0,
                    "first_observed_at": None,
                    "last_observed_at": None,
                    "status": "active",
                }
                for slot in weekly_slots
            ],
        }

    return {
        "kind": "none",
        "status": "unconfigured",
        "timezone": timezone_name,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "slot_count": 0,
        "active_slot_count": 0,
        "slots": [],
    }


def schedule_model_has_definition(schedule_model):
    """Return True if a schedule model contains a usable schedule definition."""
    if not isinstance(schedule_model, dict):
        return False
    if schedule_model.get("kind") == "interval" and schedule_model.get("interval_minutes"):
        return True
    if schedule_model.get("kind") in {"daily", "weekly"} and schedule_model.get("slots"):
        return True
    return False


def refresh_schedule_summary(ds_state):
    """Refresh the cached schedule summary for one datastore state."""
    learned_group_count = 0
    active_slot_count = 0
    for group_state in (ds_state.get("backup_groups") or {}).values():
        learned_model = group_state.get("learned_schedule_model")
        effective_model = group_state.get("schedule_model") or {}
        if schedule_model_has_definition(learned_model):
            learned_group_count += 1
        active_slot_count += coerce_int(effective_model.get("active_slot_count")) or 0

    ds_state["schedule_summary"] = {
        "learned_group_count": learned_group_count,
        "active_slot_count": active_slot_count,
    }


def purge_ignored_backup_groups(ds_state, config, datastore_id):
    """Remove ignored backup groups from persisted datastore state."""
    backup_groups = ds_state.get("backup_groups") or {}
    removed = False
    for group_key in list(backup_groups.keys()):
        group_state = backup_groups[group_key]
        if not is_group_ignored(
            config,
            datastore_id,
            group_state.get("namespace"),
            group_state.get("backup_type"),
            group_state.get("backup_id"),
        ):
            continue
        del backup_groups[group_key]
        removed = True

    if removed:
        refresh_schedule_summary(ds_state)


def sync_group_rule_from_schedule(rule, datastore_id, group_state, schedule_model):
    """Update an unlocked group rule from a learned schedule model."""
    rule["datastore_id"] = datastore_id
    rule["namespace"] = group_state.get("namespace") or ""
    rule["backup_type"] = group_state.get("backup_type") or ""
    rule["backup_id"] = str(group_state.get("backup_id", ""))
    rule["display_name"] = group_state.get("display_name")
    rule["timezone"] = schedule_model.get("timezone") or rule.get("timezone") or "local"
    rule["updated_at"] = datetime.now(timezone.utc).isoformat()
    rule["updated_by"] = "learning"

    if schedule_model.get("kind") == "interval" and schedule_model.get("interval_minutes"):
        rule["schedule_kind"] = "interval"
        rule["interval_minutes"] = schedule_model["interval_minutes"]
        rule["daily_slots"] = []
        rule["weekly_slots"] = []
        return

    if schedule_model.get("kind") == "daily" and schedule_model.get("slots"):
        rule["schedule_kind"] = "daily"
        rule["interval_minutes"] = None
        rule["daily_slots"] = normalize_daily_slots(schedule_model.get("slots"))
        rule["weekly_slots"] = []
        return

    if schedule_model.get("kind") == "weekly" and schedule_model.get("slots"):
        rule["schedule_kind"] = "weekly"
        rule["interval_minutes"] = None
        rule["daily_slots"] = []
        rule["weekly_slots"] = normalize_weekly_slots(schedule_model.get("slots"))
        return

    rule["schedule_kind"] = "none"
    rule["interval_minutes"] = None
    rule["daily_slots"] = []
    rule["weekly_slots"] = []


def hours_since(iso_str):
    """Hours elapsed since an ISO timestamp."""
    dt = parse_iso(iso_str)
    if not dt:
        return float("inf")
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


def snapshot_to_local_occurrence(snapshot, tzinfo):
    """Convert a stored snapshot record into a timezone-aware occurrence."""
    backup_time = coerce_int(snapshot.get("backup_time"))
    if backup_time is None:
        return None

    local_dt = datetime.fromtimestamp(backup_time, timezone.utc).astimezone(tzinfo)
    return {
        "backup_time": backup_time,
        "local_dt": local_dt,
        "local_date": local_dt.date().isoformat(),
        "weekday": local_dt.weekday(),
        "minute_of_day": (local_dt.hour * 60) + local_dt.minute,
        "size": snapshot.get("size"),
        "protected": bool(snapshot.get("protected", False)),
        "comment": snapshot.get("comment"),
    }


def cluster_day_occurrences(occurrences, tolerance_minutes):
    """Cluster same-weekday occurrences by time-of-day."""
    clusters = []
    for occurrence in sorted(occurrences, key=lambda item: item["minute_of_day"]):
        placed = False
        for cluster in clusters:
            if abs(occurrence["minute_of_day"] - cluster["minute_of_day"]) <= tolerance_minutes:
                cluster["occurrences"].append(occurrence)
                cluster["minute_of_day"] = int(
                    round(statistics.median(item["minute_of_day"] for item in cluster["occurrences"]))
                )
                placed = True
                break
        if not placed:
            clusters.append({
                "minute_of_day": occurrence["minute_of_day"],
                "occurrences": [occurrence],
            })
    return clusters


def find_recent_due(slot, now_local):
    """Return the most recent scheduled due datetime for a learned slot."""
    due_time = dt_time(slot["minute_of_day"] // 60, slot["minute_of_day"] % 60)
    days_since = (now_local.weekday() - slot["weekday"]) % 7
    due_date = now_local.date() - timedelta(days=days_since)
    due_dt = datetime.combine(due_date, due_time, tzinfo=now_local.tzinfo)
    if due_dt > now_local:
        due_dt -= timedelta(days=7)
    return due_dt


def format_interval_minutes(interval_minutes):
    """Format an interval in minutes for human-readable output."""
    hours, minutes = divmod(interval_minutes, 60)
    if hours and minutes:
        return f"every {hours}h {minutes}m"
    if hours:
        return f"every {hours}h"
    return f"every {minutes}m"


def detect_interval_schedule(occurrences, config, now_local, tzinfo):
    """Detect frequent interval-based backup schedules."""
    learning_cfg = config.get("schedule_learning") or {}
    tolerance_minutes = max(coerce_int(learning_cfg.get("time_tolerance_minutes")) or 30, 5)
    min_occurrences = max(coerce_int(learning_cfg.get("min_occurrences")) or 2, 2)
    if len(occurrences) < max(6, min_occurrences * 3):
        return None

    sorted_occurrences = sorted(occurrences, key=lambda item: item["local_dt"])
    gaps = []
    for previous, current in zip(sorted_occurrences, sorted_occurrences[1:]):
        gap_minutes = int(round((current["local_dt"] - previous["local_dt"]).total_seconds() / 60))
        if gap_minutes > 0:
            gaps.append(gap_minutes)

    if len(gaps) < max(4, min_occurrences * 2):
        return None

    candidate_interval = int(round(statistics.median(gaps)))
    if candidate_interval > INTERVAL_MODEL_MAX_MINUTES:
        return None

    matching_gaps = [gap for gap in gaps if abs(gap - candidate_interval) <= tolerance_minutes]
    if len(matching_gaps) < max(4, min_occurrences * 2):
        return None
    if (len(matching_gaps) / len(gaps)) < 0.75:
        return None

    return {
        "kind": "interval",
        "status": "learned",
        "timezone": str(tzinfo),
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "slot_count": 1,
        "active_slot_count": 1,
        "interval_minutes": candidate_interval,
        "interval_human": format_interval_minutes(candidate_interval),
        "sample_count": len(matching_gaps),
        "first_observed_at": sorted_occurrences[0]["local_dt"].isoformat(),
        "last_observed_at": sorted_occurrences[-1]["local_dt"].isoformat(),
        "status_reason": "interval",
        "slots": [],
    }


def detect_daily_schedule(occurrences, config, tzinfo):
    """Detect daily recurring backup windows independent of weekday."""
    learning_cfg = config.get("schedule_learning") or {}
    tolerance_minutes = max(coerce_int(learning_cfg.get("time_tolerance_minutes")) or 30, 5)
    min_occurrences = max(coerce_int(learning_cfg.get("min_occurrences")) or 2, 2)
    if len(occurrences) < max(4, min_occurrences * 3):
        return None

    day_clusters = cluster_day_occurrences(occurrences, tolerance_minutes)
    learned_slots = []
    now_local = datetime.now(tzinfo)
    stale_after_days = max(coerce_int(learning_cfg.get("stale_after_days")) or 8, 1)
    for cluster in day_clusters:
        cluster_occurrences = sorted(cluster["occurrences"], key=lambda item: item["local_dt"])
        distinct_dates = []
        distinct_weekdays = set()
        for occurrence in cluster_occurrences:
            if occurrence["local_date"] not in distinct_dates:
                distinct_dates.append(occurrence["local_date"])
            distinct_weekdays.add(occurrence["weekday"])

        if len(distinct_dates) < max(3, min_occurrences * 2):
            continue
        if len(distinct_weekdays) < 4:
            continue

        last_local = cluster_occurrences[-1]["local_dt"]
        age_days = (now_local - last_local).total_seconds() / 86400
        learned_slots.append({
            "slot_key": f"daily:{cluster['minute_of_day']}",
            "minute_of_day": cluster["minute_of_day"],
            "time": format_schedule_time(cluster["minute_of_day"]),
            "sample_count": len(distinct_dates),
            "first_observed_at": cluster_occurrences[0]["local_dt"].isoformat(),
            "last_observed_at": last_local.isoformat(),
            "status": "active" if age_days <= (1 + stale_after_days) else "stale",
        })

    learned_slots.sort(key=lambda item: item["minute_of_day"])
    if not learned_slots:
        return None

    active_slot_count = sum(1 for slot in learned_slots if slot["status"] == "active")
    return {
        "kind": "daily",
        "status": "learned" if active_slot_count else "stale",
        "timezone": str(tzinfo),
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "slot_count": len(learned_slots),
        "active_slot_count": active_slot_count,
        "interval_minutes": None,
        "interval_human": None,
        "slots": learned_slots,
    }


def evaluate_schedule_model(group_state, config, tzinfo):
    """Derive conservative weekly schedule slots from observed snapshots."""
    learning_cfg = config.get("schedule_learning") or {}
    history_window_days = max(coerce_int(learning_cfg.get("history_window_days")) or 60, 7)
    min_occurrences = max(coerce_int(learning_cfg.get("min_occurrences")) or 2, 2)
    tolerance_minutes = max(coerce_int(learning_cfg.get("time_tolerance_minutes")) or 30, 15)
    stale_after_days = max(coerce_int(learning_cfg.get("stale_after_days")) or 8, 1)
    now_utc = datetime.now(timezone.utc)
    history_cutoff = now_utc - timedelta(days=history_window_days)

    recent_snapshots = [
        snapshot
        for snapshot in normalize_snapshot_entries(group_state.get("observed_snapshots"))
        if coerce_int(snapshot.get("backup_time")) is not None
        and datetime.fromtimestamp(snapshot["backup_time"], timezone.utc) >= history_cutoff
    ]
    occurrences = [
        occurrence
        for occurrence in (snapshot_to_local_occurrence(snapshot, tzinfo) for snapshot in recent_snapshots)
        if occurrence is not None
    ]

    grouped_by_weekday = {}
    for occurrence in occurrences:
        grouped_by_weekday.setdefault(occurrence["weekday"], []).append(occurrence)

    now_local = datetime.now(tzinfo)
    interval_model = detect_interval_schedule(occurrences, config, now_local, tzinfo)
    if interval_model is not None:
        last_local = parse_iso(interval_model.get("last_observed_at"))
        age_days = (now_local - last_local).total_seconds() / 86400 if last_local else float("inf")
        interval_model["status"] = "active" if age_days <= max(1, stale_after_days) else "stale"
        interval_model["active_slot_count"] = 1 if interval_model["status"] == "active" else 0
        return interval_model

    daily_model = detect_daily_schedule(occurrences, config, tzinfo)
    if daily_model is not None:
        return daily_model

    learned_slots = []
    for weekday, day_occurrences in grouped_by_weekday.items():
        for cluster in cluster_day_occurrences(day_occurrences, tolerance_minutes):
            cluster_occurrences = sorted(cluster["occurrences"], key=lambda item: item["local_dt"])
            distinct_dates = []
            for occurrence in cluster_occurrences:
                if occurrence["local_date"] not in distinct_dates:
                    distinct_dates.append(occurrence["local_date"])

            if len(distinct_dates) < min_occurrences:
                continue

            last_local = cluster_occurrences[-1]["local_dt"]
            age_days = (now_local - last_local).total_seconds() / 86400
            learned_slots.append({
                "slot_key": f"{weekday}:{cluster['minute_of_day']}",
                "weekday": weekday,
                "weekday_name": weekday_name(weekday),
                "minute_of_day": cluster["minute_of_day"],
                "time": format_schedule_time(cluster["minute_of_day"]),
                "sample_count": len(distinct_dates),
                "first_observed_at": cluster_occurrences[0]["local_dt"].isoformat(),
                "last_observed_at": last_local.isoformat(),
                "status": "active" if age_days <= (7 + stale_after_days) else "stale",
            })

    learned_slots.sort(key=lambda item: (item["weekday"], item["minute_of_day"]))
    active_slot_count = sum(1 for slot in learned_slots if slot["status"] == "active")
    if active_slot_count:
        status = "learned"
    elif learned_slots:
        status = "stale"
    else:
        status = "learning" if recent_snapshots else "insufficient-history"

    return {
        "kind": "weekly" if learned_slots else "none",
        "status": status,
        "timezone": str(tzinfo),
        "evaluated_at": now_utc.isoformat(),
        "slot_count": len(learned_slots),
        "active_slot_count": active_slot_count,
        "interval_minutes": None,
        "interval_human": None,
        "slots": learned_slots,
    }


def build_missed_slot_alert(ds, group_state, slot, due_dt_local, same_day_occurrences):
    """Create an alert for a missed learned backup window."""
    datastore_name = ds.get("name", ds.get("id", "unknown"))
    group_label = group_state.get("display_name") or f"{group_state['backup_type']}/{group_state['backup_id']}"
    namespace = group_state.get("namespace") or "root"
    message = (
        f"Datastore '{datastore_name}' missed the learned backup window for '{group_label}' "
        f"(namespace '{namespace}') on {slot['weekday_name']} around {slot['time']} "
        f"{slot['timezone']}. Last matching backup: {slot['last_observed_at']}."
    )
    if same_day_occurrences:
        off_schedule = ", ".join(
            occurrence["local_dt"].strftime("%H:%M")
            for occurrence in same_day_occurrences
        )
        message += f" Off-schedule snapshots exist on the same day at: {off_schedule}."

    return Alert(
        datastore_name,
        "Missed Backup Window",
        message,
        priority=4,
        tags=["warning", "calendar", "package"],
        key=(
            f"{ds.get('id', 'unknown')}:missed_slot:"
            f"{group_state['namespace']}:{group_state['backup_type']}:"
            f"{group_state['backup_id']}:{slot['slot_key']}:{due_dt_local.date().isoformat()}"
        ),
        scope="group",
        group_rule_key=group_state.get("group_rule_key"),
    )


def build_missed_interval_alert(ds, group_state, schedule_model, now_local, last_local):
    """Create an alert for a missed interval-based backup schedule."""
    datastore_name = ds.get("name", ds.get("id", "unknown"))
    group_label = group_state.get("display_name") or f"{group_state['backup_type']}/{group_state['backup_id']}"
    namespace = group_state.get("namespace") or "root"
    interval_human = schedule_model.get("interval_human") or format_interval_minutes(schedule_model["interval_minutes"])
    message = (
        f"Datastore '{datastore_name}' missed the expected interval backups for '{group_label}' "
        f"(namespace '{namespace}'). Expected cadence: {interval_human} {schedule_model.get('timezone')}. "
        f"Last observed backup: {last_local.isoformat()}. Current time: {now_local.isoformat()}."
    )
    return Alert(
        datastore_name,
        "Missed Backup Interval",
        message,
        priority=4,
        tags=["warning", "calendar", "package"],
        key=(
            f"{ds.get('id', 'unknown')}:missed_interval:"
            f"{group_state['namespace']}:{group_state['backup_type']}:"
            f"{group_state['backup_id']}:{last_local.date().isoformat()}:{schedule_model['interval_minutes']}"
        ),
        scope="group",
        group_rule_key=group_state.get("group_rule_key"),
    )


def evaluate_missed_backup_alerts(ds, group_state, schedule_model, config, tzinfo):
    """Evaluate learned schedule slots for missed backup windows."""
    learning_cfg = config.get("schedule_learning") or {}
    tolerance_minutes = max(coerce_int(learning_cfg.get("time_tolerance_minutes")) or 30, 15)
    due_grace_minutes = max(coerce_int(learning_cfg.get("due_grace_minutes")) or 30, 15)
    now_local = datetime.now(tzinfo)

    occurrences = [
        occurrence
        for occurrence in (
            snapshot_to_local_occurrence(snapshot, tzinfo)
            for snapshot in normalize_snapshot_entries(group_state.get("observed_snapshots"))
        )
        if occurrence is not None
    ]

    alerts = []
    if schedule_model.get("kind") == "interval" and schedule_model.get("interval_minutes"):
        latest_occurrence = max(occurrences, key=lambda item: item["local_dt"], default=None)
        if latest_occurrence:
            due_dt_local = latest_occurrence["local_dt"] + timedelta(minutes=schedule_model["interval_minutes"])
            if now_local > due_dt_local + timedelta(minutes=due_grace_minutes):
                alerts.append(build_missed_interval_alert(
                    ds,
                    group_state,
                    schedule_model,
                    now_local,
                    latest_occurrence["local_dt"],
                ))
        return alerts

    if schedule_model.get("kind") == "daily":
        for learned_slot in schedule_model.get("slots") or []:
            if learned_slot.get("status") != "active":
                continue

            due_time = dt_time(learned_slot["minute_of_day"] // 60, learned_slot["minute_of_day"] % 60)
            due_dt_local = datetime.combine(now_local.date(), due_time, tzinfo=now_local.tzinfo)
            if due_dt_local > now_local:
                due_dt_local -= timedelta(days=1)

            window_start = due_dt_local - timedelta(minutes=tolerance_minutes)
            window_end = due_dt_local + timedelta(minutes=due_grace_minutes)
            if now_local <= window_end:
                continue

            matching_occurrence = next(
                (
                    occurrence
                    for occurrence in occurrences
                    if window_start <= occurrence["local_dt"] <= window_end
                ),
                None,
            )
            if matching_occurrence:
                continue

            same_day_occurrences = [
                occurrence
                for occurrence in occurrences
                if occurrence["local_dt"].date() == due_dt_local.date()
            ]
            alerts.append(build_missed_slot_alert(
                ds,
                group_state,
                {**learned_slot, "weekday_name": "Daily", "timezone": schedule_model.get("timezone", str(tzinfo))},
                due_dt_local,
                same_day_occurrences,
            ))
        return alerts

    for learned_slot in schedule_model.get("slots") or []:
        if learned_slot.get("status") != "active":
            continue

        due_dt_local = find_recent_due(learned_slot, now_local)
        window_start = due_dt_local - timedelta(minutes=tolerance_minutes)
        window_end = due_dt_local + timedelta(minutes=due_grace_minutes)
        if now_local <= window_end:
            continue

        matching_occurrence = next(
            (
                occurrence
                for occurrence in occurrences
                if window_start <= occurrence["local_dt"] <= window_end
            ),
            None,
        )
        if matching_occurrence:
            continue

        same_day_occurrences = [
            occurrence
            for occurrence in occurrences
            if occurrence["local_dt"].date() == due_dt_local.date()
        ]
        alerts.append(build_missed_slot_alert(
            ds,
            group_state,
            {**learned_slot, "timezone": schedule_model.get("timezone", str(tzinfo))},
            due_dt_local,
            same_day_occurrences,
        ))

    return alerts


def ensure_datastore_state(state, ds_id, name):
    """Ensure the persistent state for one datastore exists and is normalized."""
    ds_state = state["datastores"].setdefault(ds_id, default_datastore_state(name))
    ds_state["name"] = name
    ds_state["inventory_summary"] = migrate_inventory_summary(ds_state.get("inventory_summary"))
    if not isinstance(ds_state.get("schedule_summary"), dict):
        ds_state["schedule_summary"] = {
            "learned_group_count": 0,
            "active_slot_count": 0,
        }

    backup_groups = ds_state.get("backup_groups")
    if not isinstance(backup_groups, dict):
        backup_groups = {}

    ds_state["backup_groups"] = {
        str(group_key): migrate_backup_group_state(group_state)
        for group_key, group_state in backup_groups.items()
    }
    return ds_state


def apply_backup_inventory_state(ds, ds_state, backup_inventory, config, group_rules=None, persist_group_rules=False):
    """Persist live backup inventory and evaluate conservative backup alerts."""
    raw_summary = migrate_inventory_summary(backup_inventory.get("summary"))
    metric_backup_count = coerce_int((ds.get("metrics") or {}).get("backup_count")) or 0
    tzinfo = get_schedule_timezone(config)
    datastore_id = ds.get("id", "")
    group_rules = group_rules if group_rules is not None else default_group_rules()
    rules_changed = False

    if raw_summary["snapshot_count"] == 0 and metric_backup_count > 0:
        return [], f"inventory skipped (metrics={metric_backup_count}, browser=0)"

    previous_summary = migrate_inventory_summary(ds_state.get("inventory_summary"))
    observed_at = datetime.now(timezone.utc).isoformat()
    current_group_keys = set()
    backup_groups = ds_state["backup_groups"]
    visible_namespaces = set()
    visible_group_count = 0
    visible_snapshot_count = 0

    for group_record in backup_inventory.get("groups") or []:
        if is_group_ignored(
            config,
            datastore_id,
            group_record.get("namespace"),
            group_record.get("backup_type"),
            group_record.get("backup_id"),
        ):
            continue

        group_key = group_record["group_key"]
        current_group_keys.add(group_key)
        existing_group = backup_groups.get(group_key, {})

        current_snapshots = normalize_snapshot_entries(
            group_record.get("current_snapshots"),
            MAX_CURRENT_SNAPSHOT_DETAILS,
        )
        observed_snapshots = merge_snapshot_histories(
            existing_group.get("observed_snapshots"),
            current_snapshots,
            MAX_OBSERVED_SNAPSHOT_HISTORY,
        )

        display_name = group_record.get("display_name") or existing_group.get("display_name")
        if not display_name:
            display_name = f"{group_record['backup_type']}/{group_record['backup_id']}"

        comment = group_record.get("comment")
        if comment is None:
            comment = existing_group.get("comment")

        visible_namespaces.add(group_record.get("namespace") or "")
        visible_group_count += 1
        visible_snapshot_count += group_record.get("backup_count", 0) or 0

        backup_groups[group_key] = {
            "namespace": group_record.get("namespace") or "",
            "backup_type": group_record["backup_type"],
            "backup_id": group_record["backup_id"],
            "display_name": display_name,
            "comment": comment,
            "first_observed_at": existing_group.get("first_observed_at") or observed_at,
            "last_observed_at": observed_at,
            "missing_since": None,
            "last_backup_at": group_record.get("last_backup_at"),
            "current_snapshot_count": group_record.get("backup_count", 0),
            "protected_snapshot_count": group_record.get("protected_snapshot_count", 0),
            "current_snapshots": current_snapshots,
            "observed_snapshots": observed_snapshots,
            "schedule_model": existing_group.get("schedule_model", {
                "status": "learning",
                "timezone": str(tzinfo),
                "evaluated_at": None,
                "slot_count": 0,
                "active_slot_count": 0,
                "slots": [],
            }),
        }

    purge_ignored_backup_groups(ds_state, config, datastore_id)

    summary = {
        "namespace_count": len(visible_namespaces),
        "group_count": visible_group_count,
        "snapshot_count": visible_snapshot_count,
    }

    for group_key, group_state in backup_groups.items():
        if group_key not in current_group_keys and not group_state.get("missing_since"):
            group_state["missing_since"] = observed_at

    ds_state["last_inventory_at"] = observed_at
    ds_state["inventory_summary"] = summary

    alerts = []
    if previous_summary["snapshot_count"] > 0 and summary["snapshot_count"] == 0:
        name = ds.get("name", ds.get("id", "unknown"))
        alerts.append(Alert(
            name,
            "All Backups Gone",
            f"Backup inventory on '{name}' is now empty. Previously observed "
            f"{previous_summary['snapshot_count']} snapshots across "
            f"{previous_summary['group_count']} groups.",
            priority=5,
            tags=["rotating_light", "package"],
            key=f"{ds.get('id', 'unknown')}:all_backups_gone",
        ))

    learned_group_count = 0
    active_slot_count = 0
    learning_enabled = (config.get("schedule_learning") or {}).get("enabled", True)
    for group_key in current_group_keys:
        group_state = backup_groups[group_key]
        rule_key, rule = ensure_group_rule(group_rules, datastore_id, group_state)
        learned_model = group_state.get("learned_schedule_model")

        if learning_enabled:
            learned_model = evaluate_schedule_model(group_state, config, tzinfo)
            if schedule_model_has_definition(learned_model) and persist_group_rules and not rule.get("locked"):
                previous_rule = json.dumps(rule, sort_keys=True)
                sync_group_rule_from_schedule(rule, datastore_id, group_state, learned_model)
                if json.dumps(rule, sort_keys=True) != previous_rule:
                    rules_changed = True

        configured_model = build_schedule_model_from_rule(rule, str(tzinfo))
        if rule.get("locked") and schedule_model_has_definition(configured_model):
            effective_model = configured_model
        elif schedule_model_has_definition(learned_model):
            effective_model = learned_model
        elif schedule_model_has_definition(configured_model):
            effective_model = configured_model
        else:
            effective_model = learned_model or configured_model

        group_state["group_rule_key"] = rule_key
        group_state["group_rule"] = normalize_group_rule(rule)
        group_state["learned_schedule_model"] = learned_model
        group_state["schedule_model"] = effective_model

        if schedule_model_has_definition(learned_model):
            learned_group_count += 1
        active_slot_count += effective_model.get("active_slot_count", 0)
        alerts.extend(evaluate_missed_backup_alerts(ds, group_state, effective_model, config, tzinfo))

    ds_state["schedule_summary"] = {
        "learned_group_count": learned_group_count,
        "active_slot_count": active_slot_count,
    }

    return (
        alerts,
        f"{summary['group_count']} groups / {summary['snapshot_count']} snapshots / "
        f"{active_slot_count} active slots",
        rules_changed,
    )


def check_datastore(ds, config, state, backup_inventory=None, group_rules=None, persist_group_rules=False):
    """Check a single datastore for problems. Returns alerts and backup status."""
    alerts = []
    ds_id = ds.get("id", "")
    name = ds.get("name", ds.get("id", "unknown"))
    ds_state = ensure_datastore_state(state, ds_id, name)
    purge_ignored_backup_groups(ds_state, config, ds_id)
    thresholds = config["thresholds"]
    metrics = ds.get("metrics") or {}
    gc = ds.get("gc") or {}
    verification = ds.get("verification") or {}
    autoscaling = ds.get("autoscaling") or {}
    immutable = ds.get("immutable_backup") or {}
    replication = ds.get("replication") or {}

    # ── Metrics unavailable (host offline) ──
    if not metrics:
        alerts.append(Alert(
            name,
            "Host Offline",
            f"Datastore '{name}' — host server is unreachable. Metrics unavailable.",
            priority=4,
            tags=["warning", "cloud"],
        ))
        return alerts, "host offline"

    # ── Storage usage ──
    used_pct = metrics.get("used_percent", 0)
    used_human = format_bytes(metrics.get("used_bytes", 0))
    avail_human = format_bytes(metrics.get("available_bytes", 0))

    if used_pct >= thresholds["storage_crit_percent"]:
        alerts.append(Alert(
            name,
            "Storage Critical",
            f"Datastore '{name}' at {used_pct}% — {used_human} used, {avail_human} free.",
            priority=5,
            tags=["rotating_light", "floppy_disk"],
        ))
    elif used_pct >= thresholds["storage_warn_percent"]:
        alerts.append(Alert(
            name,
            "Storage Warning",
            f"Datastore '{name}' at {used_pct}% — {used_human} used, {avail_human} free.",
            priority=3,
            tags=["warning", "floppy_disk"],
        ))

    # ── GC status ──
    if gc.get("status") == "error":
        alerts.append(Alert(
            name,
            "GC Failed",
            f"Garbage collection failed on '{name}'. Last run: {gc.get('last_run', 'N/A')}",
            priority=4,
            tags=["x", "broom"],
        ))
    elif gc.get("status") == "never":
        alerts.append(Alert(
            name,
            "GC Never Ran",
            f"Garbage collection has never run on '{name}'.",
            priority=3,
            tags=["warning", "broom"],
        ))
    elif hours_since(gc.get("last_run")) > thresholds["gc_max_age_hours"]:
        h = hours_since(gc.get("last_run"))
        alerts.append(Alert(
            name,
            "GC Overdue",
            f"GC on '{name}' last ran {h:.0f}h ago (threshold: {thresholds['gc_max_age_hours']}h).",
            priority=3,
            tags=["warning", "broom"],
        ))

    # ── Verification status ──
    if verification.get("status") == "error":
        alerts.append(Alert(
            name,
            "Verification Failed",
            f"Data verification failed on '{name}'! Backup integrity may be compromised. "
            f"Last run: {verification.get('last_run', 'N/A')}",
            priority=5,
            tags=["rotating_light", "shield"],
        ))
    elif verification.get("status") == "never":
        alerts.append(Alert(
            name,
            "Verification Never Ran",
            f"Data verification has never run on '{name}'.",
            priority=3,
            tags=["warning", "shield"],
        ))
    elif hours_since(verification.get("last_run")) > thresholds["verification_max_age_days"] * 24:
        d = hours_since(verification.get("last_run")) / 24
        alerts.append(Alert(
            name,
            "Verification Overdue",
            f"Verification on '{name}' last ran {d:.0f} days ago "
            f"(threshold: {thresholds['verification_max_age_days']}d).",
            priority=3,
            tags=["warning", "shield"],
        ))

    backup_status = "inventory unavailable"
    if backup_inventory is not None:
        backup_alerts, backup_status, rules_changed = apply_backup_inventory_state(
            ds,
            ds_state,
            backup_inventory,
            config,
            group_rules=group_rules,
            persist_group_rules=persist_group_rules,
        )
        alerts.extend(backup_alerts)
        if rules_changed and persist_group_rules and group_rules is not None:
            save_group_rules(group_rules)

    # ── Immutable backup disable warning ──
    if immutable.get("disable_requested"):
        alerts.append(Alert(
            name,
            "Immutable Backup Disable Pending",
            f"Immutable backups on '{name}' have a pending disable request!",
            priority=4,
            tags=["warning", "lock"],
        ))

    # ── Replication status ──
    if replication.get("enabled"):
        interval_minutes = coerce_int(replication.get("interval_minutes")) or 0
        last_sync = replication.get("last_sync")
        if not last_sync:
            alerts.append(Alert(
                name,
                "Replication Never Synced",
                f"Replication is enabled on '{name}' but no completed sync has been observed yet.",
                priority=4,
                tags=["warning", "arrow_repeat"],
            ))
        elif interval_minutes > 0:
            threshold_minutes = max(interval_minutes + 30, interval_minutes * 2)
            lag_minutes = hours_since(last_sync) * 60
            if lag_minutes > threshold_minutes:
                alerts.append(Alert(
                    name,
                    "Replication Stale",
                    f"Replication on '{name}' last synced {lag_minutes:.0f} minutes ago "
                    f"(interval: {interval_minutes}m, threshold: {threshold_minutes}m).",
                    priority=4,
                    tags=["warning", "arrow_repeat"],
                ))

    return alerts, backup_status


def format_bytes(b):
    """Format bytes to human readable (base-1000 / SI units)."""
    if not b:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(b) < 1000:
            return f"{b:.1f} {unit}"
        b /= 1000
    return f"{b:.1f} PB"


# ─── ntfy Integration ────────────────────────────────────────────────────────

def _ntfy_header_safe(value):
    """Encode a string so it is safe to use in an HTTP header (latin-1 range).

    Characters outside latin-1 are replaced with their closest ASCII
    representation via unicode normalization, and any remaining non-latin-1
    characters are dropped rather than causing a UnicodeEncodeError.
    """
    import unicodedata
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("latin-1", errors="ignore").decode("latin-1")


def send_ntfy(config, alert):
    """Send a single alert via ntfy."""
    # Check if ntfy is configured - don't send to external services without user configuration
    ntfy_topic = config.get("ntfy_topic", "").strip()
    if not ntfy_topic:
        print("  [INFO] ntfy_topic not configured, skipping push notification")
        return False
        
    ntfy_url = config.get("ntfy_url", "https://ntfy.sh").strip()
    if not ntfy_url:
        print("  [INFO] ntfy_url not configured, skipping push notification")
        return False
        
    headers = {
        "Title": _ntfy_header_safe(f"PBS: {alert.title}"),
        "Priority": str(alert.priority),
        "Tags": _ntfy_header_safe(",".join(alert.tags) if alert.tags else "backup"),
    }
    if config.get("ntfy_token"):
        headers["Authorization"] = f"Bearer {config['ntfy_token']}"

    url = f"{ntfy_url}/{ntfy_topic}"
    try:
        resp = requests.post(url, data=alert.message.encode("utf-8"), headers=headers, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"  [ERROR] Failed to send ntfy: {e}", file=sys.stderr)
        return False


def is_quiet_hours(config):
    """Check if current time is within quiet hours."""
    qh = config.get("quiet_hours", {})
    if not qh.get("enabled"):
        return False
    now = datetime.now().strftime("%H:%M")
    start = qh.get("start", "22:00")
    end = qh.get("end", "07:00")
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end


def should_alert(config, state, alert_key):
    """Check if we should send this alert (cooldown check)."""
    last = state.get("last_alerts", {}).get(alert_key)
    if not last:
        return True
    cooldown = config.get("alert_cooldown_minutes", 60)
    elapsed = (datetime.now(timezone.utc) - parse_iso(last)).total_seconds() / 60
    return elapsed >= cooldown


# ─── Main ────────────────────────────────────────────────────────────────────

def run_check(config, state):
    """Run a single monitoring check cycle."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n{'='*60}")
    print(f"PBS Monitor Check — {now}")
    print(f"{'='*60}")

    # Check API health first
    try:
        health = requests.get(f"{config['api_base']}/health", timeout=10).json()
        api_status = health.get("status", "unknown")
        print(f"API Health: {api_status}")
        if api_status != "ok":
            alert = Alert(
                "platform",
                "API Unhealthy",
                f"remote-backups.com API health check returned: {api_status}",
                priority=4,
                tags=["warning", "cloud"],
            )
            if should_alert(config, state, alert.key):
                send_ntfy(config, alert)
                state.setdefault("last_alerts", {})[alert.key] = datetime.now(timezone.utc).isoformat()
    except requests.RequestException as e:
        print(f"API Health: UNREACHABLE ({e})")
        alert = Alert(
            "platform",
            "API Unreachable",
            f"remote-backups.com API is not reachable: {e}",
            priority=5,
            tags=["rotating_light", "cloud"],
        )
        if should_alert(config, state, alert.key):
            send_ntfy(config, alert)
            state.setdefault("last_alerts", {})[alert.key] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return

    # Fetch datastores
    try:
        datastores = fetch_datastores(config)
    except (requests.RequestException, RuntimeError) as e:
        print(f"Failed to fetch datastores: {e}")
        alert = Alert(
            "platform",
            "Monitoring API Error",
            f"Failed to fetch datastores: {e}",
            priority=4,
            tags=["x", "cloud"],
        )
        if should_alert(config, state, alert.key):
            send_ntfy(config, alert)
            state.setdefault("last_alerts", {})[alert.key] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        return

    print(f"Datastores found: {len(datastores)}")

    quiet = is_quiet_hours(config)
    if quiet:
        print("Quiet hours active — suppressing low-priority alerts")

    group_rules = load_group_rules()
    all_alerts = []
    for ds in datastores:
        name = ds.get("name", ds.get("id", "?"))
        metrics = ds.get("metrics") or {}
        backup_inventory = None
        backup_inventory_error = None
        if metrics:
            try:
                backup_inventory = fetch_backup_inventory(config, ds.get("id", ""))
            except (requests.RequestException, RuntimeError) as e:
                backup_inventory_error = str(e)

        alerts, backup_status = check_datastore(
            ds,
            config,
            state,
            backup_inventory=backup_inventory,
            group_rules=group_rules,
            persist_group_rules=True,
        )

        status_str = "OFFLINE" if not metrics else f"{metrics.get('used_percent', '?')}%"
        gc_status = (ds.get("gc") or {}).get("status", "?")
        verify_status = (ds.get("verification") or {}).get("status", "?")
        print(
            f"\n  [{name}] Storage: {status_str} | GC: {gc_status} | "
            f"Verify: {verify_status} | Backups: {backup_status}"
        )
        if backup_inventory_error:
            print(f"    • Backup inventory unavailable: {backup_inventory_error}")

        if alerts:
            for a in alerts:
                print(f"    ⚠ {a.title}: {a.message}")
            all_alerts.extend(alerts)
        else:
            print(f"    ✓ All checks passed")

    # Send alerts
    sent = 0
    skipped = 0
    for alert in all_alerts:
        if quiet and alert.priority < config["quiet_hours"].get("min_priority", 4):
            skipped += 1
            continue
        if not should_alert(config, state, alert.key):
            skipped += 1
            continue
        if send_ntfy(config, alert):
            state.setdefault("last_alerts", {})[alert.key] = datetime.now(timezone.utc).isoformat()
            sent += 1

    print(f"\nAlerts: {len(all_alerts)} detected, {sent} sent, {skipped} skipped (cooldown/quiet)")
    save_state(state)


def main():
    parser = argparse.ArgumentParser(description="PBS Monitor — Alerting for remote-backups.com")
    parser.add_argument(
        "--daemon", type=int, metavar="SECONDS",
        help="Run continuously, checking every N seconds",
    )
    args = parser.parse_args()

    config = load_config()
    state = load_state()

    if args.daemon:
        print(f"Running in daemon mode, interval: {args.daemon}s")
        running = True

        def handle_signal(sig, frame):
            nonlocal running
            print("\nShutting down...")
            running = False

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        while running:
            run_check(config, state)
            for _ in range(args.daemon):
                if not running:
                    break
                time.sleep(1)
    else:
        run_check(config, state)


if __name__ == "__main__":
    main()
