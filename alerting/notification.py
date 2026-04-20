"""ntfy notification sending and quiet-hours / cooldown helpers.

Imports only from the standard library plus the alerting.normalization module.
"""

import ipaddress
import json
import os
import socket
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

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
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] Could not write notification log: {exc}", file=sys.stderr)


# ── SSRF guard ────────────────────────────────────────────────────────────────

_PRIVATE_NETWORKS_MONITOR = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("fc00::/7"),
]


def _validate_ntfy_url_monitor(url: str) -> None:
    """Raise ValueError if url is not a safe, public http/https endpoint."""
    try:
        parsed = urlparse(url)
    except Exception:
        raise ValueError("Invalid ntfy URL.")
    if parsed.scheme not in ("http", "https"):
        raise ValueError("ntfy URL must use http or https.")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("ntfy URL must contain a hostname.")
    try:
        results = socket.getaddrinfo(hostname, None)
    except OSError:
        raise ValueError("ntfy URL hostname could not be resolved.")
    for _family, _type, _proto, _canonname, sockaddr in results:
        raw_addr = sockaddr[0]
        try:
            addr = ipaddress.ip_address(raw_addr)
        except ValueError:
            continue
        for net in _PRIVATE_NETWORKS_MONITOR:
            if addr in net:
                raise ValueError("ntfy URL must not point to a private or reserved address.")


# ── ntfy sender ───────────────────────────────────────────────────────────────

def send_ntfy(config, alert):
    """Send a single alert via the ntfy push-notification service."""
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

    try:
        _validate_ntfy_url_monitor(ntfy_url)
    except ValueError as exc:
        print(f"  [ERROR] ntfy URL rejected (SSRF guard): {exc}", file=sys.stderr)
        return False

    url = f"{ntfy_url}/{ntfy_topic}"
    try:
        resp = requests.post(url, data=alert.message.encode("utf-8"), headers=headers, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"  [ERROR] Failed to send ntfy: {e}", file=sys.stderr)
        return False


# ── Quiet-hours and cooldown checks ──────────────────────────────────────────

def is_quiet_hours(config):
    """Return True if the current time falls within the configured quiet-hours window."""
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
    """Return True if the alert cooldown has expired for *alert_key*."""
    from alerting.schedule import parse_iso  # avoid top-level circular import
    last = state.get("last_alerts", {}).get(alert_key)
    if not last:
        return True
    cooldown = config.get("alert_cooldown_minutes", 60)
    elapsed = (datetime.now(timezone.utc) - parse_iso(last)).total_seconds() / 60
    return elapsed >= cooldown
