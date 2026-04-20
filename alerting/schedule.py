"""Schedule detection, evaluation, and missed-backup alert building.

Pure computation — no file I/O.  Imports only from the standard library and
from alerting.normalization.
"""

import statistics
from datetime import datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from alerting.normalization import (
    coerce_int,
    format_schedule_time,
    weekday_name,
    normalize_daily_slots,
    normalize_weekly_slots,
    normalize_snapshot_entries,
)


# Maximum interval length (in minutes) that the interval detector will accept.
# Schedules longer than this are treated as weekly-pattern backups instead.
INTERVAL_MODEL_MAX_MINUTES = 360


# ── Alert data class ──────────────────────────────────────────────────────────

class Alert:
    """Represents a single monitoring alert."""

    # Priority levels: 1=min, 2=low, 3=default, 4=high, 5=urgent
    def __init__(self, datastore_name, title, message, priority=3, tags=None,
                 key=None, scope="datastore", group_rule_key=None):
        self.datastore_name = datastore_name
        self.title = title
        self.message = message
        self.priority = priority
        self.tags = tags or []
        self.key = key or f"{datastore_name}:{title}"
        self.scope = scope
        self.group_rule_key = group_rule_key


# ── Timezone helpers ──────────────────────────────────────────────────────────

def parse_iso(iso_str):
    """Parse an ISO timestamp string to a timezone-aware datetime, or None."""
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def get_schedule_timezone(config):
    """Return the tzinfo used for schedule learning from config."""
    timezone_name = (config.get("schedule_learning") or {}).get("timezone", "local")
    if not timezone_name or timezone_name == "local":
        return datetime.now().astimezone().tzinfo

    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        print(f"[WARN] Unknown schedule timezone '{timezone_name}', using local timezone.")
        return datetime.now().astimezone().tzinfo


def format_interval_minutes(interval_minutes):
    """Format an interval in minutes as a human-readable string."""
    hours, minutes = divmod(interval_minutes, 60)
    if hours and minutes:
        return f"every {hours}h {minutes}m"
    if hours:
        return f"every {hours}h"
    return f"every {minutes}m"


# ── Schedule model builders ───────────────────────────────────────────────────

def build_schedule_model_from_rule(rule, fallback_timezone):
    """Build an effective schedule model dict from a persisted group rule."""
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
            "interval_anchor_minute": rule.get("interval_anchor_minute"),
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
    """Refresh the cached schedule summary counts for one datastore state dict."""
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


# ── Occurrence helpers ────────────────────────────────────────────────────────

def snapshot_to_local_occurrence(snapshot, tzinfo):
    """Convert a stored snapshot record into a timezone-aware occurrence dict."""
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


def hours_since(iso_str):
    """Return the number of hours elapsed since an ISO timestamp."""
    dt = parse_iso(iso_str)
    if not dt:
        return float("inf")
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


# ── Clustering and due-time computation ──────────────────────────────────────

def cluster_day_occurrences(occurrences, tolerance_minutes):
    """Cluster same-weekday occurrences by time-of-day proximity."""
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
    """Return the most-recent scheduled due datetime for a weekly learned slot."""
    due_time = dt_time(slot["minute_of_day"] // 60, slot["minute_of_day"] % 60)
    days_since = (now_local.weekday() - slot["weekday"]) % 7
    due_date = now_local.date() - timedelta(days=days_since)
    due_dt = datetime.combine(due_date, due_time, tzinfo=now_local.tzinfo)
    if due_dt > now_local:
        due_dt -= timedelta(days=7)
    return due_dt


def compute_anchor_aligned_due(anchor_minute, interval_minutes, now_local):
    """Return the most-recently due anchor-aligned interval time before or at now."""
    day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    anchor_dt = day_start + timedelta(minutes=anchor_minute)
    if anchor_dt > now_local:
        anchor_dt -= timedelta(days=1)
    delta_minutes = (now_local - anchor_dt).total_seconds() / 60
    k = int(delta_minutes // interval_minutes)
    return anchor_dt + timedelta(minutes=k * interval_minutes)


def compute_next_expected_backup(group_state, schedule_model, tzinfo):
    """Return an ISO string for the next expected backup time, or None."""
    if not schedule_model_has_definition(schedule_model):
        return None

    now_local = datetime.now(tzinfo)

    if schedule_model.get("kind") == "interval" and schedule_model.get("interval_minutes"):
        interval_minutes = schedule_model["interval_minutes"]
        anchor_minute = schedule_model.get("interval_anchor_minute")

        if anchor_minute is not None:
            due_dt = compute_anchor_aligned_due(anchor_minute, interval_minutes, now_local)
            return (due_dt + timedelta(minutes=interval_minutes)).isoformat()

        last_backup_at = group_state.get("last_backup_at")
        if not last_backup_at:
            return None
        last_backup_dt = parse_iso(last_backup_at).astimezone(tzinfo)
        return (last_backup_dt + timedelta(minutes=interval_minutes)).isoformat()

    if schedule_model.get("kind") in ("daily", "weekly"):
        next_dt = None
        for slot in schedule_model.get("slots") or []:
            if slot.get("status") != "active":
                continue
            due_time = dt_time(slot["minute_of_day"] // 60, slot["minute_of_day"] % 60)
            if schedule_model["kind"] == "daily":
                candidate = datetime.combine(now_local.date(), due_time, tzinfo=now_local.tzinfo)
                if candidate <= now_local:
                    candidate += timedelta(days=1)
            else:
                days_until = (slot["weekday"] - now_local.weekday()) % 7
                candidate = datetime.combine(
                    now_local.date() + timedelta(days=days_until),
                    due_time,
                    tzinfo=now_local.tzinfo,
                )
                if candidate <= now_local:
                    candidate += timedelta(weeks=1)
            if next_dt is None or candidate < next_dt:
                next_dt = candidate
        return next_dt.isoformat() if next_dt else None

    return None


# ── Schedule detection ────────────────────────────────────────────────────────

def detect_interval_schedule(occurrences, config, now_local, tzinfo):
    """Detect frequent interval-based backup schedules from occurrence history."""
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
    """Derive a conservative schedule model from observed snapshot history."""
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


# ── Missed-backup alert builders ─────────────────────────────────────────────

def build_missed_slot_alert(ds, group_state, slot, due_dt_local, same_day_occurrences):
    """Create an Alert for a missed learned backup window slot."""
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
    """Create an Alert for a missed interval-based backup schedule."""
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
    """Evaluate learned schedule slots and return any missed-backup alerts."""
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
        interval_minutes = schedule_model["interval_minutes"]
        anchor_minute = schedule_model.get("interval_anchor_minute")

        if anchor_minute is not None:
            due_dt = compute_anchor_aligned_due(anchor_minute, interval_minutes, now_local)
            if now_local > due_dt + timedelta(minutes=due_grace_minutes):
                window_start = due_dt - timedelta(minutes=tolerance_minutes)
                window_end = due_dt + timedelta(minutes=due_grace_minutes)
                matching_occurrence = next(
                    (
                        occurrence
                        for occurrence in occurrences
                        if window_start <= occurrence["local_dt"] <= window_end
                    ),
                    None,
                )
                if not matching_occurrence:
                    next_due_dt = due_dt + timedelta(minutes=interval_minutes)
                    late_coverage = any(
                        window_end < o["local_dt"] <= next_due_dt
                        for o in occurrences
                    )
                    if not late_coverage:
                        last_local = max(
                            (o["local_dt"] for o in occurrences),
                            default=now_local,
                        )
                        alerts.append(build_missed_interval_alert(
                            ds, group_state, schedule_model, now_local, last_local,
                        ))
        else:
            latest_occurrence = max(occurrences, key=lambda item: item["local_dt"], default=None)
            if latest_occurrence:
                due_dt_local = latest_occurrence["local_dt"] + timedelta(minutes=interval_minutes)
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
