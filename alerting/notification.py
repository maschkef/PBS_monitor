"""ntfy notification sending and quiet-hours / cooldown helpers.

Imports only from the standard library plus the alerting.normalization module.
"""

import json
import os
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests


# ── Byte formatter (used in alert messages) ───────────────────────────────────

def format_bytes(b):
    """Format bytes to human-readable string (base-1000 / SI units)."""
    if not b:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(b) < 1000:
            return f"{b:.1f} {unit}"
        b /= 1000
    return f"{b:.1f} PB"


# ── ntfy header safety ────────────────────────────────────────────────────────

def _ntfy_header_safe(value):
    """Return *value* with only latin-1-safe characters (for HTTP headers)."""
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("latin-1", errors="ignore").decode("latin-1")


# ── Notification log ──────────────────────────────────────────────────────────

_MAX_NOTIFICATION_LOG_ENTRIES = 500


def append_notification_log(log_path, entry):
    """Append one entry to the notification log, capping at the configured max.

    Silently ignores write errors so a broken log never prevents alerting.
    """
    log_path = Path(log_path)
    try:
        if log_path.exists():
            with open(log_path) as f:
                entries = json.load(f)
            if not isinstance(entries, list):
                entries = []
        else:
            entries = []
        entries.append(entry)
        if len(entries) > _MAX_NOTIFICATION_LOG_ENTRIES:
            entries = entries[-_MAX_NOTIFICATION_LOG_ENTRIES:]
        with open(log_path, "w") as f:
            json.dump(entries, f, indent=2)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  [WARN] Could not write notification log: {exc}", file=sys.stderr)


# ── ntfy sender ───────────────────────────────────────────────────────────────

class NtfyDeliveryError(Exception):
    pass

def send_ntfy(config, alert):
    """
    Send a single alert via the ntfy push-notification service.
    Returns True if successfully sent, False if skipped (not configured).
    Raises NtfyDeliveryError if delivery fails.
    """
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
    effective_token = os.environ.get("NTFY_TOKEN", "").strip() or config.get("ntfy_token", "")
    if effective_token:
        headers["Authorization"] = f"Bearer {effective_token}"

    url = f"{ntfy_url}/{quote(ntfy_topic, safe='')}"
    try:
        resp = requests.post(url, data=alert.message.encode("utf-8"), headers=headers, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"  [ERROR] Failed to send ntfy: {e}", file=sys.stderr)
        raise NtfyDeliveryError(f"Failed to send ntfy: {e}") from e


# ── Quiet-hours and cooldown checks ──────────────────────────────────────────

def is_quiet_hours(config):
    """Return True if the current time falls within the configured quiet-hours window."""
    qh = config.get("quiet_hours", {})
    if not qh.get("enabled"):
        return False
    now = datetime.now(timezone.utc).astimezone().strftime("%H:%M")
    start = qh.get("start", "22:00")
    end = qh.get("end", "07:00")
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end


def should_alert(config, state, alert_key):
    """Return True if the alert cooldown has expired for *alert_key*."""
    from alerting.schedule import parse_iso  # avoid top-level circular import
    now_utc = datetime.now(timezone.utc)

    suppress_map = state.get("alert_suppress_until") or {}
    suppress_until = suppress_map.get(alert_key)
    if suppress_until:
        try:
            suppress_dt = parse_iso(suppress_until)
        except (TypeError, ValueError):
            suppress_dt = None
        if suppress_dt is not None:
            if suppress_dt.tzinfo is None:
                suppress_dt = suppress_dt.replace(tzinfo=timezone.utc)
            if now_utc < suppress_dt.astimezone(timezone.utc):
                return False
        suppress_map.pop(alert_key, None)

    last = state.get("last_alerts", {}).get(alert_key)
    if not last:
        return True
    cooldown = config.get("alert_cooldown_minutes", 60)
    elapsed = (now_utc - parse_iso(last)).total_seconds() / 60
    return elapsed >= cooldown
