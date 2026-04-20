"""Alerting UI helpers — build visual alerting payloads from persisted state.

These functions are consumed by Flask route handlers in app.py.  They accept
file-system paths and alert_monitor explicitly so they can be unit-tested or
called from outside a Flask request context.
"""

import copy
import json
from datetime import datetime
from pathlib import Path

import requests

from alerting import monitor as alert_monitor


def load_visual_alerting_config(config_path: Path) -> dict:
    """Load alerting configuration without creating files as a side effect."""
    config = copy.deepcopy(alert_monitor.DEFAULT_CONFIG)
    if not config_path.exists():
        return config

    with open(config_path) as f:
        raw_config = json.load(f)

    merged = {**config, **raw_config}
    merged["thresholds"] = {**config["thresholds"], **raw_config.get("thresholds", {})}
    merged["quiet_hours"] = {**config["quiet_hours"], **raw_config.get("quiet_hours", {})}
    merged["schedule_learning"] = {
        **config["schedule_learning"],
        **raw_config.get("schedule_learning", {}),
    }
    merged["ignored_groups"] = alert_monitor.normalize_ignored_groups(raw_config.get("ignored_groups"))
    return merged


def load_visual_alerting_state(state_path: Path) -> tuple:
    """Load alerting state for preview purposes.

    Returns ``(state_dict, source_label)`` where *source_label* is either
    ``"persisted"`` or ``"ephemeral"``.
    """
    if not state_path.exists():
        return alert_monitor.default_state(), "ephemeral"
    with open(state_path) as f:
        return alert_monitor.migrate_state(json.load(f)), "persisted"


def load_visual_group_rules() -> tuple:
    """Load persisted per-group schedule rules for preview and editing.

    Returns ``(group_rules_dict, source_label)``.
    """
    if not alert_monitor.GROUP_RULES_PATH.exists():
        return alert_monitor.default_group_rules(), "ephemeral"
    return alert_monitor.load_group_rules(), "persisted"


def priority_to_health(priority: int) -> str:
    """Map an alert priority integer to a dashboard health label."""
    if priority >= 4:
        return "critical"
    if priority >= 3:
        return "warning"
    return "healthy"


def serialize_schedule_model(schedule_model) -> dict:
    """Serialize a schedule model dict for frontend consumption."""
    if not isinstance(schedule_model, dict):
        schedule_model = {}

    return {
        "kind": schedule_model.get("kind", "none"),
        "status": schedule_model.get("status", "unconfigured"),
        "timezone": schedule_model.get("timezone"),
        "interval_minutes": schedule_model.get("interval_minutes"),
        "interval_anchor_minute": schedule_model.get("interval_anchor_minute"),
        "interval_human": schedule_model.get("interval_human"),
        "slot_count": schedule_model.get("slot_count", 0),
        "active_slot_count": schedule_model.get("active_slot_count", 0),
        "slots": [
            {
                "weekday": slot.get("weekday"),
                "weekday_name": slot.get("weekday_name"),
                "minute_of_day": slot.get("minute_of_day"),
                "time": slot.get("time"),
                "sample_count": slot.get("sample_count", 0),
                "status": slot.get("status"),
            }
            for slot in schedule_model.get("slots") or []
        ],
        "last_observed_at": schedule_model.get("last_observed_at"),
    }


def collect_schedule_groups(ds_state: dict, tzinfo=None) -> list:
    """Collect schedule information for all backup groups in one datastore."""
    if tzinfo is None:
        tzinfo = datetime.now().astimezone().tzinfo

    group_alert_counts: dict = {}
    for alert in ds_state.get("active_group_alerts") or []:
        rule_key = alert.get("group_rule_key")
        if not rule_key:
            continue
        group_alert_counts[rule_key] = group_alert_counts.get(rule_key, 0) + 1

    groups = []
    for group_state in (ds_state.get("backup_groups") or {}).values():
        group_rule = alert_monitor.normalize_group_rule(group_state.get("group_rule"))
        configured_schedule = alert_monitor.build_schedule_model_from_rule(
            group_rule,
            (group_state.get("schedule_model") or {}).get("timezone") or "local",
        )
        effective_model = group_state.get("schedule_model") or {}
        next_expected_at = alert_monitor.compute_next_expected_backup(
            group_state, effective_model, tzinfo
        )
        groups.append({
            "rule_key": group_state.get("group_rule_key"),
            "label": group_state.get("display_name") or f"{group_state.get('backup_type')}/{group_state.get('backup_id')}",
            "datastore_id": group_rule.get("datastore_id"),
            "namespace": group_state.get("namespace") or "root",
            "backup_type": group_state.get("backup_type"),
            "backup_id": group_state.get("backup_id"),
            "last_backup_at": group_state.get("last_backup_at"),
            "next_expected_at": next_expected_at,
            "locked": bool(group_rule.get("locked")),
            "group_alert_count": group_alert_counts.get(group_state.get("group_rule_key"), 0),
            "group_rule": group_rule,
            "effective_schedule": serialize_schedule_model(effective_model),
            "learned_schedule": serialize_schedule_model(group_state.get("learned_schedule_model") or {}),
            "configured_schedule": serialize_schedule_model(configured_schedule),
        })

    groups.sort(key=lambda item: (-item["group_alert_count"], -int(item["locked"]), item["namespace"], item["label"]))
    return groups


def build_visual_alerting(
    detail: dict,
    alerting_config: dict,
    alerting_state: dict,
    group_rules: dict,
    rules_source: str,
    state_path: Path,
    *,
    fetch_inventory: bool = True,
) -> dict:
    """Evaluate alerting status for one datastore without sending notifications.

    Set *fetch_inventory* to False to skip live backup-inventory API calls and
    rely on persisted alerting state only (used for lightweight auto-refresh).
    """
    backup_inventory = None
    inventory_error = None
    if fetch_inventory and detail.get("metrics"):
        try:
            backup_inventory = alert_monitor.fetch_backup_inventory(alerting_config, detail.get("id", ""))
        except (requests.RequestException, RuntimeError) as e:
            inventory_error = str(e)

    alerts, backup_status = alert_monitor.check_datastore(
        detail,
        alerting_config,
        alerting_state,
        backup_inventory=backup_inventory,
        group_rules=group_rules,
        persist_group_rules=False,
    )
    ds_state = alerting_state["datastores"].get(detail.get("id", ""), {})
    schedule_summary = ds_state.get("schedule_summary") or {}

    serialized_alerts = [
        {
            "title": alert.title,
            "message": alert.message,
            "priority": alert.priority,
            "tags": alert.tags,
            "scope": getattr(alert, "scope", "datastore"),
            "group_rule_key": getattr(alert, "group_rule_key", None),
        }
        for alert in alerts
    ]
    max_priority = max((alert["priority"] for alert in serialized_alerts), default=0)
    ds_state["active_group_alerts"] = [
        alert
        for alert in serialized_alerts
        if alert.get("scope") == "group" and alert.get("group_rule_key")
    ]

    tzinfo = alert_monitor.get_schedule_timezone(alerting_config)
    ds_id = detail.get("id", "")

    _bg_display_names: dict = {}
    for group_state in (ds_state.get("backup_groups") or {}).values():
        rk = alert_monitor.make_rule_key(
            ds_id,
            group_state.get("namespace"),
            group_state.get("backup_type"),
            group_state.get("backup_id"),
        )
        if group_state.get("display_name"):
            _bg_display_names[rk] = group_state["display_name"]

    ds_ignored_groups = []
    for ig in (alerting_config.get("ignored_groups") or []):
        if ig.get("datastore_id") != ds_id:
            continue
        enriched_ig = dict(ig)
        if not enriched_ig.get("display_name"):
            lookup_key = alert_monitor.make_rule_key(
                ds_id,
                ig.get("namespace"),
                ig.get("backup_type"),
                ig.get("backup_id"),
            )
            enriched_ig["display_name"] = _bg_display_names.get(lookup_key)
        ds_ignored_groups.append(enriched_ig)

    return {
        "health": priority_to_health(max_priority),
        "alerts": serialized_alerts,
        "alert_count": len(serialized_alerts),
        "max_priority": max_priority,
        "backup_status": backup_status,
        "inventory_error": inventory_error,
        "state_source": "persisted" if state_path.exists() else "ephemeral",
        "rules_source": rules_source,
        "ignored_groups": ds_ignored_groups,
        "schedule_learning": {
            "learned_group_count": schedule_summary.get("learned_group_count", 0),
            "active_slot_count": schedule_summary.get("active_slot_count", 0),
            "groups": collect_schedule_groups(ds_state, tzinfo=tzinfo),
        },
    }
