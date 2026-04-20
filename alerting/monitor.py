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
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from alerting.normalization import (
    coerce_int,
    unix_to_iso,
    make_rule_key,
    normalize_weekly_slots,
    normalize_daily_slots,
    normalize_group_rule,
    normalize_ignored_group,  # noqa: F401 — re-export for callers using alert_monitor.*
    normalize_ignored_groups,
    is_group_ignored,
    normalize_snapshot_entries,
    merge_snapshot_histories,
    default_datastore_state,
    default_state,
    default_group_rules,
    migrate_inventory_summary,
    migrate_backup_group_state,
    migrate_state,
    migrate_group_rules,
)
from alerting.schedule import (
    Alert,
    get_schedule_timezone,
    format_schedule_time,   # noqa: F401 — re-export
    weekday_name,           # noqa: F401 — re-export
    format_interval_minutes,  # noqa: F401 — re-export
    build_schedule_model_from_rule,
    schedule_model_has_definition,
    refresh_schedule_summary,
    hours_since,
    snapshot_to_local_occurrence,   # noqa: F401 — re-export
    cluster_day_occurrences,        # noqa: F401 — re-export
    find_recent_due,                # noqa: F401 — re-export
    compute_anchor_aligned_due,     # noqa: F401 — re-export
    detect_interval_schedule,       # noqa: F401 — re-export
    detect_daily_schedule,          # noqa: F401 — re-export
    evaluate_schedule_model,
    build_missed_slot_alert,        # noqa: F401 — re-export
    build_missed_interval_alert,    # noqa: F401 — re-export
    evaluate_missed_backup_alerts,
)
from alerting.notification import (
    format_bytes,
    _ntfy_header_safe,              # noqa: F401 — re-export
    append_notification_log,
    _validate_ntfy_url_monitor,     # noqa: F401 — re-export
    send_ntfy,
    is_quiet_hours,
    should_alert,
)

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
NOTIFICATION_LOG_PATH = DATA_DIR / "notification_log.json"

load_dotenv(ENV_PATH)

STATE_VERSION = 2
MAX_CURRENT_SNAPSHOT_DETAILS = 24
MAX_OBSERVED_SNAPSHOT_HISTORY = 1000
MAX_NOTIFICATION_LOG_ENTRIES = 500
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
    "notification_priorities": {
        "warning": 4,   # high — storage warnings, overdue GC/verification, missed backups
        "critical": 5,  # urgent — critical storage, verification failure, datastore offline
    },
    "schedule_learning": {
        "enabled": True,
        "timezone": "local",
        "history_window_days": 60,
        "min_occurrences": 2,
        "time_tolerance_minutes": 30,
        "due_grace_minutes": 30,
        "stale_after_days": 8,
        "snapshot_retention_count": 24,
    },
    "alert_cooldown_minutes": 60,
    "daemon_interval_seconds": 1800,  # 30 minutes default for daemon mode
}


# ─── Config / state I/O ─────────────────────────────────────────────────────

def load_config():
    """Load config, create from example if missing."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        # Merge with defaults for new keys
        merged = {**DEFAULT_CONFIG, **cfg}
        merged["thresholds"] = {**DEFAULT_CONFIG["thresholds"], **cfg.get("thresholds", {})}
        merged["quiet_hours"] = {**DEFAULT_CONFIG["quiet_hours"], **cfg.get("quiet_hours", {})}
        merged["notification_priorities"] = {
            **DEFAULT_CONFIG["notification_priorities"],
            **cfg.get("notification_priorities", {}),
        }
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


# ─── Inventory helpers ───────────────────────────────────────────────────────

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


def build_backup_group_record(namespace, group, group_snapshots, snapshot_cap=MAX_CURRENT_SNAPSHOT_DETAILS):
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
        "current_snapshots": normalize_snapshot_entries(group_snapshots, snapshot_cap),
    }


def extract_namespace_backup_groups(namespace, namespace_data, snapshot_cap=MAX_CURRENT_SNAPSHOT_DETAILS):
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
        groups.append(build_backup_group_record(namespace, group, group_snapshots, snapshot_cap))

    for (backup_type, backup_id), group_snapshots in snapshots_by_group.items():
        synthetic_group = {
            "backup_type": backup_type,
            "backup_id": backup_id,
            "last_backup": group_snapshots[0]["backup_time"] if group_snapshots else None,
            "backup_count": len(group_snapshots),
            "comment": None,
        }
        groups.append(build_backup_group_record(namespace, synthetic_group, group_snapshots, snapshot_cap))

    return groups


def fetch_backup_inventory(config, datastore_id):
    """Fetch full PBS backup inventory for a datastore, grouped by namespace."""
    snapshot_cap = max(1, coerce_int(
        (config.get("schedule_learning") or {}).get("snapshot_retention_count")
    ) or MAX_CURRENT_SNAPSHOT_DETAILS)
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
        groups.extend(extract_namespace_backup_groups(namespace_value, namespace_data, snapshot_cap))

    snapshot_count = sum(group["backup_count"] for group in groups)
    return {
        "summary": {
            "namespace_count": len(namespace_entries),
            "group_count": len(groups),
            "snapshot_count": snapshot_count,
        },
        "groups": groups,
    }


# ─── Group rules ─────────────────────────────────────────────────────────────

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


# ─── Schedule / rule sync helpers ────────────────────────────────────────────

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


# ─── Datastore state management ──────────────────────────────────────────────

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
    snapshot_cap = max(1, coerce_int(
        (config.get("schedule_learning") or {}).get("snapshot_retention_count")
    ) or MAX_CURRENT_SNAPSHOT_DETAILS)

    if raw_summary["snapshot_count"] == 0 and metric_backup_count > 0:
        return [], f"inventory skipped (metrics={metric_backup_count}, browser=0)", False

    previous_summary = migrate_inventory_summary(ds_state.get("inventory_summary"))
    observed_at = datetime.now(timezone.utc).isoformat()
    current_group_keys = set()
    backup_groups = ds_state["backup_groups"]
    visible_namespaces = set()
    visible_group_count = 0
    visible_snapshot_count = 0
    # Track groups seen for the first time this run so we can skip missed-backup
    # evaluation: on first observation we load the full historical snapshot list
    # but have no prior baseline, which would produce false-positive alerts.
    new_group_keys: set = set()

    # Prune config for snapshot disappearance detection.
    # keep_last is the minimum number of snapshots PBS must retain per group after
    # any prune run, regardless of other bucket settings.  If the observed count
    # drops below min(prev_count, keep_last) the loss cannot be explained by
    # normal pruning and we raise an alert.
    prune_cfg = ds.get("prune") or {}
    keep_last = max(0, coerce_int(prune_cfg.get("keep_last")) or 0)
    snapshot_disappearances: list = []  # (group_key, curr_count, expected_min, vanished_times, group_record)

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
        if not existing_group.get("first_observed_at"):
            new_group_keys.add(group_key)

        current_snapshots = normalize_snapshot_entries(
            group_record.get("current_snapshots"),
            snapshot_cap,
        )

        # ── Detect unexpected snapshot loss relative to keep_last ──
        if group_key not in new_group_keys and keep_last > 0:
            prev_count = coerce_int(existing_group.get("current_snapshot_count")) or 0
            curr_count = (
                coerce_int(group_record.get("backup_count"))
                if coerce_int(group_record.get("backup_count")) is not None
                else len(current_snapshots)
            )
            expected_min = min(prev_count, keep_last)
            if curr_count < expected_min:
                prev_times = {
                    s["backup_time"]
                    for s in (existing_group.get("current_snapshots") or [])
                    if s.get("backup_time")
                }
                curr_times = {s["backup_time"] for s in current_snapshots if s.get("backup_time")}
                vanished = sorted(prev_times - curr_times, reverse=True)
                snapshot_disappearances.append(
                    (group_key, curr_count, expected_min, vanished, group_record)
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
    ds_name = ds.get("name", ds.get("id", "unknown"))

    # ── Snapshot disappearance alerts ──
    for group_key, curr_count, expected_min, vanished_times, group_record in snapshot_disappearances:
        group_state = backup_groups.get(group_key, {})
        group_display = (
            group_state.get("display_name")
            or f"{group_record['backup_type']}/{group_record['backup_id']}"
        )
        ns = group_record.get("namespace") or ""
        ns_str = f" in namespace '{ns}'" if ns else ""
        loss = expected_min - curr_count
        time_detail = ""
        if vanished_times:
            sample = ", ".join(
                datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                for t in vanished_times[:3]
            )
            extra = f" (+{len(vanished_times) - 3} more)" if len(vanished_times) > 3 else ""
            time_detail = f" Missing: {sample}{extra}."
        alerts.append(Alert(
            ds_name,
            "Snapshots Unexpectedly Removed",
            f"{loss} snapshot(s) for '{group_display}'{ns_str} on '{ds_name}' disappeared "
            f"outside the prune policy (keep_last={keep_last}, expected >={expected_min}, "
            f"got {curr_count}).{time_detail}",
            priority=4,
            tags=["warning", "package"],
            key=f"{ds.get('id', 'unknown')}:{group_key}:unexpected_snapshot_removal",
        ))

    # ── All Backups Gone ──
    if previous_summary["snapshot_count"] > 0 and summary["snapshot_count"] == 0:
        alerts.append(Alert(
            ds_name,
            "All Backups Gone",
            f"Backup inventory on '{ds_name}' is now empty. Previously observed "
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
        if group_key in new_group_keys:
            # First observation this run: we have snapshot history but no prior baseline.
            # Skip missed-backup evaluation to avoid false-positive alerts on startup.
            continue
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


# ─── Main check loop ─────────────────────────────────────────────────────────

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
            print("    ✓ All checks passed")

    # Send alerts
    prio_cfg = config.get("notification_priorities", {})
    warn_prio = max(1, min(5, int(prio_cfg.get("warning") or 4)))
    crit_prio = max(1, min(5, int(prio_cfg.get("critical") or 5)))
    sent = 0
    skipped = 0
    for alert in all_alerts:
        # Apply configured severity → priority mapping (4 = warning tier, 5 = critical tier)
        if alert.priority >= 5:
            alert.priority = crit_prio
        elif alert.priority >= 4:
            alert.priority = warn_prio
        if quiet and alert.priority < config["quiet_hours"].get("min_priority", 4):
            skipped += 1
            continue
        if not should_alert(config, state, alert.key):
            skipped += 1
            continue
        if send_ntfy(config, alert):
            now_iso = datetime.now(timezone.utc).isoformat()
            state.setdefault("last_alerts", {})[alert.key] = now_iso
            append_notification_log(NOTIFICATION_LOG_PATH, {
                "timestamp": now_iso,
                "source": "alerting",
                "title": alert.title,
                "message": alert.message,
                "priority": alert.priority,
                "datastore_name": alert.datastore_name,
                "alert_key": alert.key,
            })
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
