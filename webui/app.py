#!/usr/bin/env python3
"""PBS Monitor Web UI — Ad-hoc status dashboard for remote-backups.com datastores."""

import contextlib
import copy
import io
import ipaddress
import json
import logging
import os
import secrets
import socket
import sys
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse

import requests
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alerting import monitor as alert_monitor  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
    storage_uri="memory://",
)


@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({"error": "Rate limit exceeded. Please slow down."}), 429

API_BASE = "https://api.remote-backups.com"
API_KEY = os.environ.get("API_KEY", "")
# Support a configurable data directory so Docker containers can mount a
# persistent volume separate from the application code.
_alerting_data_env = os.environ.get("ALERTING_DATA_DIR", "").strip()
_alerting_data_dir = Path(_alerting_data_env) if _alerting_data_env else (PROJECT_ROOT / "alerting")
ALERTING_CONFIG_PATH = _alerting_data_dir / "config.json"
ALERTING_STATE_PATH = _alerting_data_dir / "state.json"
ALERTING_LOG_PATH = _alerting_data_dir / "notification_log.json"

WEBUI_HOST = os.environ.get("WEBUI_HOST", "127.0.0.1")
try:
    WEBUI_PORT = int(os.environ.get("WEBUI_PORT", "5111"))
except ValueError:
    WEBUI_PORT = 5111
WEBUI_READ_ONLY = os.environ.get("WEBUI_READ_ONLY", "").lower() in ("1", "true", "yes")
FLASK_DEBUG = os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
WEBUI_PASSWORD = os.environ.get("WEBUI_PASSWORD", "").strip()
WEBUI_SECRET_KEY = os.environ.get("WEBUI_SECRET_KEY", "").strip()


def _configure_secret_key() -> str:
    """Return the Flask secret key from environment, or a volatile fallback.

    If WEBUI_PASSWORD is set (auth is active) but WEBUI_SECRET_KEY is not,
    sessions will not survive a server restart.  A warning is printed so the
    operator knows to set WEBUI_SECRET_KEY for a stable deployment.
    """
    if WEBUI_SECRET_KEY:
        return WEBUI_SECRET_KEY
    if WEBUI_PASSWORD:
        print(
            "[WARN] WEBUI_PASSWORD is set but WEBUI_SECRET_KEY is not. "
            "Sessions will be invalidated on every restart. "
            "Set WEBUI_SECRET_KEY to a random hex string for stable sessions.",
            file=sys.stderr,
        )
    return secrets.token_hex(32)


app.secret_key = _configure_secret_key()
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"


@app.after_request
def add_security_headers(response):
    """Inject security headers on every response."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "font-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none';"
    )
    return response


# Sentinel returned to the browser instead of actual secret values.
# The frontend must submit this exact string back for the server to recognise
# that the user did not change the secret (i.e., preserve the stored value).
_TOKEN_SENTINEL = "***CONFIGURED***"

# ── Audit logging ─────────────────────────────────────────────────────────────
_AUDIT_LOG_PATH = os.environ.get("AUDIT_LOG_PATH", "").strip()
_audit_logger = logging.getLogger("pbs_monitor.audit")
_audit_logger.setLevel(logging.INFO)
_audit_logger.propagate = False
if _AUDIT_LOG_PATH:
    _audit_handler: logging.Handler = logging.FileHandler(_AUDIT_LOG_PATH, encoding="utf-8")
else:
    _audit_handler = logging.StreamHandler(sys.stderr)
_audit_handler.setFormatter(logging.Formatter("%(message)s"))
_audit_logger.addHandler(_audit_handler)


def _audit(action: str, **kwargs) -> None:
    """Emit a structured JSON audit log line for a security-relevant event."""
    record: dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "remote_addr": request.remote_addr,
        "user_agent": (request.headers.get("User-Agent", "") or "")[:200],
    }
    record.update(kwargs)
    _audit_logger.info(json.dumps(record, separators=(",", ":")))


def auth_enabled() -> bool:
    """Return True when password-based authentication is configured."""
    return bool(WEBUI_PASSWORD)


_PRIVATE_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("fc00::/7"),
]


def _validate_ntfy_url(url: str) -> None:
    """Raise ValueError if url is not a safe, public http/https endpoint.

    Prevents SSRF by rejecting loopback, link-local, private-range, and
    cloud-metadata addresses as well as non-http/https schemes.
    """
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
        for net in _PRIVATE_NETWORKS:
            if addr in net:
                raise ValueError("ntfy URL must not point to a private or reserved address.")


def _get_or_create_csrf_token() -> str:
    """Return the per-session CSRF token, creating it if necessary."""
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]


def require_auth(f):
    """Decorator: redirect to /login (or return 401 for API routes) when auth is enabled."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not auth_enabled():
            return f(*args, **kwargs)
        if not session.get("authenticated"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def require_csrf(f):
    """Decorator: validate X-CSRF-Token header for all state-changing routes.

    Only enforced when auth is enabled (CSRF is only meaningful in a
    session-authenticated context).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not auth_enabled():
            return f(*args, **kwargs)
        token = request.headers.get("X-CSRF-Token", "")
        expected = session.get("csrf_token", "")
        if not token or not secrets.compare_digest(token, expected):
            return jsonify({"error": "CSRF validation failed"}), 403
        return f(*args, **kwargs)
    return decorated


def _redact_config(cfg: dict) -> dict:
    """Return a copy of cfg safe for browser consumption.

    Secrets are replaced with a sentinel so the frontend can distinguish
    'token is configured' from 'token is empty' without the actual value
    ever leaving the server.
    """
    redacted = dict(cfg)
    redacted["ntfy_token_set"] = bool(redacted.get("ntfy_token"))
    if redacted.get("ntfy_token"):
        redacted["ntfy_token"] = _TOKEN_SENTINEL
    return redacted


def api_get(path, params=None):
    """Make authenticated GET request to the monitoring API."""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(f"{API_BASE}{path}", headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def api_get_public(path):
    """Make unauthenticated GET request to public endpoints."""
    resp = requests.get(f"{API_BASE}{path}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def format_bytes(b):
    """Format bytes to human readable string (base-1000 / SI units)."""
    if b is None:
        return "N/A"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(b) < 1000:
            return f"{b:.1f} {unit}"
        b /= 1000
    return f"{b:.1f} PB"


def format_binary_bytes(b):
    """Format bytes using IEC units for technical backup browser data."""
    if b is None:
        return "N/A"
    if b == 0:
        return "0 B"
    value = float(b)
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PiB"


def unix_to_iso(timestamp):
    """Convert a UNIX timestamp to an ISO 8601 string."""
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def normalize_backup_file(file_entry):
    """Normalize file metadata returned by backup browsing."""
    size = file_entry.get("size")
    return {
        "filename": file_entry.get("filename", "unknown"),
        "size": size,
        "size_human": format_binary_bytes(size),
        "csum": file_entry.get("csum"),
    }


def normalize_backup_snapshot(snapshot):
    """Normalize a single PBS snapshot returned by backup browsing."""
    files = [normalize_backup_file(file_entry) for file_entry in snapshot.get("files") or []]
    backup_time = snapshot.get("backup_time")

    # The remote-backups.com monitoring API does not include a per-snapshot verification
    # field in BackupSnapshotEntry. Verification status is only available at datastore level.
    # If the field is ever added to the API response, this code will handle it correctly.
    raw_verification = snapshot.get("verification")
    if isinstance(raw_verification, dict):
        verification_state = raw_verification.get("state")
    elif isinstance(raw_verification, str):
        verification_state = raw_verification
    else:
        verification_state = None

    return {
        "backup_type": snapshot.get("backup_type"),
        "backup_id": str(snapshot.get("backup_id", "")),
        "backup_time": backup_time,
        "backup_time_iso": unix_to_iso(backup_time),
        "size": snapshot.get("size"),
        "size_human": format_binary_bytes(snapshot.get("size")),
        "protected": bool(snapshot.get("protected", False)),
        "comment": snapshot.get("comment"),
        "file_count": len(files),
        "files": files,
        "verification_state": verification_state,
    }


def is_trivial_zfs_recv_entry(entry):
    """Return True for internal-looking ZFS receive metadata snapshots.

    The monitoring API can expose tiny snapshot records under zfs-recv even
    when the user does not actively use ZFS receive. Hide the section only if
    every entry looks like such a minimal metadata artifact.
    """
    referenced_bytes = entry.get("referencedBytes")
    if referenced_bytes is None:
        return False

    return (
        entry.get("type") == "snapshot"
        and entry.get("usedBytes") == 0
        and entry.get("depth") == -1
        and referenced_bytes <= 131072
    )


def should_hide_zfs_recv(payload):
    """Suppress zfs-recv if it only contains trivial metadata entries."""
    return isinstance(payload, list) and payload and all(
        is_trivial_zfs_recv_entry(entry) for entry in payload
    )


def normalize_backup_group(group, snapshots):
    """Attach snapshots to a PBS backup group and add display helpers."""
    last_backup = group.get("last_backup")
    sorted_snapshots = sorted(
        snapshots,
        key=lambda item: item.get("backup_time") or 0,
        reverse=True,
    )
    latest_comment = next(
        (
            snapshot.get("comment")
            for snapshot in sorted_snapshots
            if snapshot.get("comment")
        ),
        None,
    )
    distinct_comments = []
    for snapshot in sorted_snapshots:
        comment = snapshot.get("comment")
        if comment and comment not in distinct_comments:
            distinct_comments.append(comment)

    return {
        "backup_type": group.get("backup_type"),
        "backup_id": str(group.get("backup_id", "")),
        "group_key": f"{group.get('backup_type', 'other')}/{group.get('backup_id', 'unknown')}",
        "display_name": latest_comment or group.get("comment"),
        "last_backup": last_backup,
        "last_backup_iso": unix_to_iso(last_backup),
        "backup_count": group.get("backup_count", len(sorted_snapshots)),
        "comment": group.get("comment"),
        "latest_comment": latest_comment,
        "distinct_comments": distinct_comments,
        "snapshot_count": len(sorted_snapshots),
        "snapshots": sorted_snapshots,
    }


def normalize_namespace(namespace_meta, namespace_data):
    """Normalize a namespace payload into a grouped browser structure."""
    snapshots_by_group = {}
    for snapshot in namespace_data.get("snapshots") or []:
        normalized_snapshot = normalize_backup_snapshot(snapshot)
        key = (normalized_snapshot["backup_type"], normalized_snapshot["backup_id"])
        snapshots_by_group.setdefault(key, []).append(normalized_snapshot)

    groups = []
    for group in namespace_data.get("groups") or []:
        key = (group.get("backup_type"), str(group.get("backup_id", "")))
        group_snapshots = snapshots_by_group.pop(key, [])
        groups.append(normalize_backup_group(group, group_snapshots))

    for (backup_type, backup_id), group_snapshots in snapshots_by_group.items():
        synthetic_group = {
            "backup_type": backup_type,
            "backup_id": backup_id,
            "last_backup": group_snapshots[0].get("backup_time") if group_snapshots else None,
            "backup_count": len(group_snapshots),
            "comment": None,
        }
        groups.append(normalize_backup_group(synthetic_group, group_snapshots))

    groups.sort(
        key=lambda item: (
            -(item.get("last_backup") or 0),
            item.get("backup_type") or "",
            item.get("backup_id") or "",
        )
    )

    namespace_value = namespace_meta.get("ns", "")
    return {
        "ns": namespace_value,
        "label": namespace_value or "root",
        "comment": namespace_meta.get("comment"),
        "group_count": len(groups),
        "snapshot_count": sum(group.get("snapshot_count", 0) for group in groups),
        "groups": groups,
    }


def time_ago(iso_str):
    """Convert ISO timestamp to human-readable 'time ago' string."""
    if not iso_str:
        return "never"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    diff = now - dt
    seconds = int(diff.total_seconds())
    if seconds < 0:
        return format_time_until(abs(seconds))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def format_time_until(seconds):
    """Format seconds until a future event."""
    if seconds < 3600:
        return f"in {seconds // 60}m"
    if seconds < 86400:
        return f"in {seconds // 3600}h"
    return f"in {seconds // 86400}d"


def time_until(iso_str):
    """Convert ISO timestamp to 'time until' string."""
    if not iso_str:
        return "N/A"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    diff = dt - now
    seconds = int(diff.total_seconds())
    if seconds < 0:
        return "overdue"
    return format_time_until(seconds)


def load_visual_alerting_config():
    """Load alerting configuration without creating files as a side effect."""
    config = copy.deepcopy(alert_monitor.DEFAULT_CONFIG)
    if not ALERTING_CONFIG_PATH.exists():
        return config

    with open(ALERTING_CONFIG_PATH) as f:
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


def load_visual_alerting_state():
    """Load alerting state for preview purposes without persisting UI changes."""
    if not ALERTING_STATE_PATH.exists():
        return alert_monitor.default_state(), "ephemeral"
    return alert_monitor.load_state(), "persisted"


def load_visual_group_rules():
    """Load persisted per-group schedule rules for preview and editing."""
    if not alert_monitor.GROUP_RULES_PATH.exists():
        return alert_monitor.default_group_rules(), "ephemeral"
    return alert_monitor.load_group_rules(), "persisted"


def read_only_guard():
    """Return a 403 JSON response when WEBUI_READ_ONLY is active, else None."""
    if WEBUI_READ_ONLY:
        return jsonify({"error": "Dashboard befindet sich im Read-Only-Modus."}), 403
    return None


def priority_to_health(priority):
    """Map an alert priority to dashboard health."""
    if priority >= 4:
        return "critical"
    if priority >= 3:
        return "warning"
    return "healthy"


def serialize_schedule_model(schedule_model):
    """Serialize a schedule model for the UI."""
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


def collect_schedule_groups(ds_state, tzinfo=None):
    """Collect schedule information for all backup groups in one datastore."""
    if tzinfo is None:
        tzinfo = datetime.now().astimezone().tzinfo

    group_alert_counts = {}
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


def build_visual_alerting(detail, alerting_config, alerting_state, group_rules, rules_source, *, fetch_inventory=True):
    """Evaluate alerting status for one datastore without sending notifications.

    Set fetch_inventory=False to skip live backup-inventory API calls and rely
    on persisted alerting state only (used for lightweight auto-refresh).
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

    # Build a lookup of display names from the current backup-group state
    _bg_display_names = {}
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
        "state_source": "persisted" if ALERTING_STATE_PATH.exists() else "ephemeral",
        "rules_source": rules_source,
        "ignored_groups": ds_ignored_groups,
        "schedule_learning": {
            "learned_group_count": schedule_summary.get("learned_group_count", 0),
            "active_slot_count": schedule_summary.get("active_slot_count", 0),
            "groups": collect_schedule_groups(ds_state, tzinfo=tzinfo),
        },
    }


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def login():
    """Login page — only active when WEBUI_PASSWORD is configured."""
    if not auth_enabled():
        return redirect(url_for("index"))

    error = None
    if request.method == "POST":
        # Validate the per-form nonce to prevent login-CSRF before checking
        # the password. Constant-time compare for both checks.
        form_nonce = request.form.get("_nonce", "")
        session_nonce = session.get("login_nonce", "")
        nonce_valid = bool(form_nonce) and bool(session_nonce) and secrets.compare_digest(
            form_nonce, session_nonce
        )
        password_valid = secrets.compare_digest(
            request.form.get("password", ""), WEBUI_PASSWORD
        )
        if nonce_valid and password_valid:
            session.clear()
            session["authenticated"] = True
            session["csrf_token"] = secrets.token_hex(32)
            _audit("login_success")
            return redirect(url_for("index"))
        _audit("login_failure")
        error = "Invalid password"

    nonce = secrets.token_hex(16)
    session["login_nonce"] = nonce
    return render_template("login.html", error=error, nonce=nonce)


@app.route("/logout", methods=["POST"])
def logout():
    """Destroy the current session."""
    _audit("logout")
    session.clear()
    return redirect(url_for("login") if auth_enabled() else url_for("index"))


@app.route("/")
@require_auth
def index():
    csrf_token = _get_or_create_csrf_token() if auth_enabled() else ""
    return render_template("index.html", csrf_token=csrf_token, auth_enabled=auth_enabled())


@app.route("/api/datastores/metrics")
@require_auth
def get_datastores_metrics():
    """Fetch frequently-changing datastore data for lightweight auto-refresh.

    Compared to /api/datastores this endpoint skips:
      - per-datastore rescale-log (saves N API calls)
      - live backup-inventory fetch in alerting (saves N × 5+ API calls)

    It still calls the per-datastore detail endpoint so that gc, verification
    and replication sync times are current.  Alerting is evaluated against the
    persisted local state rather than a fresh inventory.
    """
    alerting_config = load_visual_alerting_config()
    alerting_state, state_source = load_visual_alerting_state()
    group_rules, rules_source = load_visual_group_rules()
    try:
        datastores = api_get("/monitoring/v1/datastores")
    except requests.RequestException as e:
        return jsonify({"error": f"API unreachable: {e}"}), 502

    enriched = []
    for ds in datastores:
        ds_id = ds["id"]

        try:
            detail = api_get(f"/monitoring/v1/datastores/{ds_id}")
        except requests.RequestException:
            detail = ds

        metrics = detail.get("metrics") or {}
        gc = detail.get("gc") or {}
        verification = detail.get("verification") or {}
        replication = detail.get("replication") or {}

        health = "healthy"
        issues = []
        used_pct = metrics.get("used_percent", 0)
        if used_pct >= 90:
            health = "critical"
            issues.append(f"Storage at {used_pct}%")
        elif used_pct >= 80:
            health = "warning"
            issues.append(f"Storage at {used_pct}%")

        if gc.get("status") == "error":
            health = "critical"
            issues.append("GC failed")
        elif gc.get("status") == "never":
            health = "warning"
            issues.append("GC never ran")

        if verification.get("status") == "error":
            health = "critical"
            issues.append("Verification failed")
        elif verification.get("status") == "never":
            health = "warning"
            issues.append("Verification never ran")

        visual_alerting = build_visual_alerting(
            detail,
            alerting_config,
            alerting_state,
            group_rules,
            rules_source,
            fetch_inventory=False,
        )
        if visual_alerting["health"] == "critical":
            health = "critical"
        elif visual_alerting["health"] == "warning" and health != "critical":
            health = "warning"

        if visual_alerting["alert_count"]:
            issues.append(f"{visual_alerting['alert_count']} active alerts")

        enriched.append({
            "id": ds_id,
            "health": health,
            "issues": issues,
            "metrics": {
                "used_bytes": metrics.get("used_bytes", 0),
                "available_bytes": metrics.get("available_bytes", 0),
                "used_percent": used_pct,
                "backup_count": metrics.get("backup_count", 0),
                "used_human": format_bytes(metrics.get("used_bytes")),
                "available_human": format_bytes(metrics.get("available_bytes")),
                "total_human": format_bytes(
                    (metrics.get("used_bytes") or 0) + (metrics.get("available_bytes") or 0)
                ),
            },
            "gc": {
                "status": gc.get("status", "unknown"),
                "last_run": gc.get("last_run"),
                "last_run_ago": time_ago(gc.get("last_run")),
                "next_scheduled": gc.get("next_scheduled"),
                "next_in": time_until(gc.get("next_scheduled")),
            },
            "verification": {
                "status": verification.get("status", "unknown"),
                "last_run": verification.get("last_run"),
                "last_run_ago": time_ago(verification.get("last_run")),
                "next_scheduled": verification.get("next_scheduled"),
                "next_in": time_until(verification.get("next_scheduled")),
            },
            "replication": {
                "enabled": replication.get("enabled", False),
                "factor": replication.get("factor", 0),
                "last_sync": replication.get("last_sync"),
                "next_sync": replication.get("next_sync"),
                "interval_minutes": replication.get("interval_minutes", 0),
            },
            "alerting": {
                **visual_alerting,
                "state_source": state_source,
            },
        })

    return jsonify(enriched)


@app.route("/api/datastores")
@require_auth
def get_datastores():
    """Fetch all datastores with full details."""
    rescale_range = request.args.get("rescale_range", "90d")
    alerting_config = load_visual_alerting_config()
    alerting_state, state_source = load_visual_alerting_state()
    group_rules, rules_source = load_visual_group_rules()
    try:
        datastores = api_get("/monitoring/v1/datastores")
    except requests.RequestException as e:
        return jsonify({"error": f"API unreachable: {e}"}), 502

    enriched = []
    for ds in datastores:
        ds_id = ds["id"]

        # Fetch detail for prune, replication, immutable info
        try:
            detail = api_get(f"/monitoring/v1/datastores/{ds_id}")
        except requests.RequestException:
            detail = ds

        # Fetch rescale log
        try:
            rescale_log = api_get(f"/monitoring/v1/datastores/{ds_id}/rescale-log?range={rescale_range}")
        except requests.RequestException:
            rescale_log = []

        metrics = detail.get("metrics") or {}
        gc = detail.get("gc") or {}
        verification = detail.get("verification") or {}
        prune = detail.get("prune") or {}
        autoscaling = detail.get("autoscaling") or {}
        immutable = detail.get("immutable_backup") or {}
        replication = detail.get("replication") or {}

        # Determine overall health
        health = "healthy"
        issues = []
        used_pct = metrics.get("used_percent", 0)
        if used_pct >= 90:
            health = "critical"
            issues.append(f"Storage at {used_pct}%")
        elif used_pct >= 80:
            health = "warning"
            issues.append(f"Storage at {used_pct}%")

        if gc.get("status") == "error":
            health = "critical"
            issues.append("GC failed")
        elif gc.get("status") == "never":
            health = "warning"
            issues.append("GC never ran")

        if verification.get("status") == "error":
            health = "critical"
            issues.append("Verification failed")
        elif verification.get("status") == "never":
            health = "warning"
            issues.append("Verification never ran")

        visual_alerting = build_visual_alerting(
            detail,
            alerting_config,
            alerting_state,
            group_rules,
            rules_source,
        )
        if visual_alerting["health"] == "critical":
            health = "critical"
        elif visual_alerting["health"] == "warning" and health != "critical":
            health = "warning"

        if visual_alerting["alert_count"]:
            issues.append(f"{visual_alerting['alert_count']} active alerts")

        enriched.append({
            "id": ds_id,
            "name": detail.get("name", "Unknown"),
            "size_gb": detail.get("size_gb", 0),
            "created_at": detail.get("created_at", ""),
            "health": health,
            "issues": issues,
            "metrics": {
                "used_bytes": metrics.get("used_bytes", 0),
                "available_bytes": metrics.get("available_bytes", 0),
                "used_percent": used_pct,
                "backup_count": metrics.get("backup_count", 0),
                "used_human": format_bytes(metrics.get("used_bytes")),
                "available_human": format_bytes(metrics.get("available_bytes")),
                "total_human": format_bytes(
                    (metrics.get("used_bytes") or 0) + (metrics.get("available_bytes") or 0)
                ),
            },
            "gc": {
                "status": gc.get("status", "unknown"),
                "last_run": gc.get("last_run"),
                "last_run_ago": time_ago(gc.get("last_run")),
                "next_scheduled": gc.get("next_scheduled"),
                "next_in": time_until(gc.get("next_scheduled")),
            },
            "verification": {
                "status": verification.get("status", "unknown"),
                "last_run": verification.get("last_run"),
                "last_run_ago": time_ago(verification.get("last_run")),
                "next_scheduled": verification.get("next_scheduled"),
                "next_in": time_until(verification.get("next_scheduled")),
            },
            "prune": {
                "schedule": prune.get("schedule", "N/A"),
                "keep_last": prune.get("keep_last", 0),
                "keep_hourly": prune.get("keep_hourly", 0),
                "keep_daily": prune.get("keep_daily", 0),
                "keep_weekly": prune.get("keep_weekly", 0),
                "keep_monthly": prune.get("keep_monthly", 0),
                "keep_yearly": prune.get("keep_yearly", 0),
            },
            "autoscaling": {
                "enabled": autoscaling.get("enabled", False),
                "scale_up_only": autoscaling.get("scale_up_only", False),
                "lower_threshold": autoscaling.get("lower_threshold_percent", 0),
                "upper_threshold": autoscaling.get("upper_threshold_percent", 0),
            },
            "immutable_backup": {
                "enabled": immutable.get("enabled", False),
                "disable_requested": immutable.get("disable_requested", False),
            },
            "replication": {
                "enabled": replication.get("enabled", False),
                "factor": replication.get("factor", 0),
                "last_sync": replication.get("last_sync"),
                "next_sync": replication.get("next_sync"),
                "interval_minutes": replication.get("interval_minutes", 0),
            },
            "alerting": {
                **visual_alerting,
                "state_source": state_source,
            },
            "rescale_log": rescale_log[:10],
        })

    return jsonify(enriched)


@app.route("/api/alerting/group-rule", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("30 per minute")
def save_group_rule():
    guard = read_only_guard()
    if guard:
        return guard
    """Persist a manual or locked schedule rule for one backup group."""
    payload = request.get_json(silent=True) or {}
    datastore_id = payload.get("datastore_id") or ""
    namespace = payload.get("namespace") or ""
    backup_type = payload.get("backup_type") or ""
    backup_id = str(payload.get("backup_id", ""))
    if not datastore_id or not backup_type or not backup_id:
        return jsonify({"error": "Missing datastore_id, backup_type or backup_id."}), 400

    rule_key = alert_monitor.make_rule_key(datastore_id, namespace, backup_type, backup_id)
    group_rules = load_visual_group_rules()[0]
    existing_rule = group_rules.setdefault("groups", {}).get(rule_key)
    rule = alert_monitor.normalize_group_rule(existing_rule)

    schedule_kind = payload.get("schedule_kind")
    if schedule_kind not in {"daily", "weekly", "interval", "none"}:
        return jsonify({"error": "schedule_kind must be daily, weekly, interval or none."}), 400

    daily_slots = alert_monitor.normalize_daily_slots(payload.get("daily_slots"))
    weekly_slots = alert_monitor.normalize_weekly_slots(payload.get("weekly_slots"))
    interval_minutes = alert_monitor.coerce_int(payload.get("interval_minutes"))
    if schedule_kind == "daily" and not daily_slots:
        return jsonify({"error": "At least one daily slot is required."}), 400
    if schedule_kind == "weekly" and not weekly_slots:
        return jsonify({"error": "At least one weekly slot is required."}), 400
    if schedule_kind == "interval" and (interval_minutes is None or interval_minutes <= 0):
        return jsonify({"error": "interval_minutes must be a positive number."}), 400

    rule.update({
        "datastore_id": datastore_id,
        "namespace": namespace,
        "backup_type": backup_type,
        "backup_id": backup_id,
        "display_name": payload.get("display_name") or rule.get("display_name"),
        "locked": bool(payload.get("locked", False)),
        "schedule_kind": schedule_kind,
        "timezone": payload.get("timezone") or rule.get("timezone") or "local",
        "daily_slots": daily_slots if schedule_kind == "daily" else [],
        "weekly_slots": weekly_slots if schedule_kind == "weekly" else [],
        "interval_minutes": interval_minutes if schedule_kind == "interval" else None,
        "interval_anchor_minute": alert_monitor.coerce_int(payload.get("interval_anchor_minute")) if schedule_kind == "interval" else None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": "user",
    })
    group_rules["groups"][rule_key] = alert_monitor.normalize_group_rule(rule)
    alert_monitor.save_group_rules(group_rules)
    _audit("group_rule_save", rule_key=rule_key, locked=rule.get("locked"), schedule_kind=schedule_kind)
    return jsonify({
        "ok": True,
        "rule_key": rule_key,
        "rule": group_rules["groups"][rule_key],
    })


@app.route("/api/alerting/ignore-group", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("30 per minute")
def ignore_group():
    guard = read_only_guard()
    if guard:
        return guard
    """Persist one backup group in the active ignored_groups config."""
    payload = request.get_json(silent=True) or {}
    datastore_id = payload.get("datastore_id") or ""
    namespace = payload.get("namespace") or ""
    backup_type = payload.get("backup_type") or ""
    backup_id = str(payload.get("backup_id", ""))
    if not datastore_id or not backup_type or not backup_id:
        return jsonify({"error": "Missing datastore_id, backup_type or backup_id."}), 400

    raw_config = {}
    if ALERTING_CONFIG_PATH.exists():
        with open(ALERTING_CONFIG_PATH) as f:
            raw_config = json.load(f)

    ignored_group = {
        "datastore_id": datastore_id,
        "namespace": namespace,
        "backup_type": backup_type,
        "backup_id": backup_id,
        "display_name": payload.get("display_name") or None,
    }
    normalized_ignored_groups = alert_monitor.normalize_ignored_groups(raw_config.get("ignored_groups"))
    already_exists = any(
        entry.get("datastore_id") == ignored_group["datastore_id"]
        and entry.get("namespace") == ignored_group["namespace"]
        and entry.get("backup_type") == ignored_group["backup_type"]
        and entry.get("backup_id") == ignored_group["backup_id"]
        for entry in normalized_ignored_groups
    )
    if not already_exists:
        normalized_ignored_groups.append(ignored_group)

    raw_config["ignored_groups"] = normalized_ignored_groups
    with open(ALERTING_CONFIG_PATH, "w") as f:
        json.dump(raw_config, f, indent=2)

    rule_key = alert_monitor.make_rule_key(datastore_id, namespace, backup_type, backup_id)
    group_rules = load_visual_group_rules()[0]
    if rule_key in group_rules.get("groups", {}):
        del group_rules["groups"][rule_key]
        alert_monitor.save_group_rules(group_rules)

    rule_key_ig = alert_monitor.make_rule_key(datastore_id, namespace, backup_type, backup_id)
    _audit("ignore_group", rule_key=rule_key_ig, already_existed=already_exists)
    return jsonify({
        "ok": True,
        "ignored_group": ignored_group,
        "already_exists": already_exists,
    })


@app.route("/api/alerting/unignore-group", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("30 per minute")
def unignore_group():
    """Remove a backup group from the ignored_groups list in config."""
    guard = read_only_guard()
    if guard:
        return guard
    payload = request.get_json(silent=True) or {}
    datastore_id = payload.get("datastore_id") or ""
    namespace = payload.get("namespace") or ""
    backup_type = payload.get("backup_type") or ""
    backup_id = str(payload.get("backup_id", ""))
    if not datastore_id or not backup_type or not backup_id:
        return jsonify({"error": "Missing datastore_id, backup_type or backup_id."}), 400

    raw_config = {}
    if ALERTING_CONFIG_PATH.exists():
        with open(ALERTING_CONFIG_PATH) as f:
            raw_config = json.load(f)

    normalized = alert_monitor.normalize_ignored_groups(raw_config.get("ignored_groups"))
    new_list = [
        ig for ig in normalized
        if not (
            (ig.get("datastore_id") or "") == datastore_id
            and (ig.get("namespace") or "") == namespace
            and (ig.get("backup_type") or "") == backup_type
            and (ig.get("backup_id") or "") == backup_id
        )
    ]
    raw_config["ignored_groups"] = new_list
    with open(ALERTING_CONFIG_PATH, "w") as f:
        json.dump(raw_config, f, indent=2)

    removed_count = len(normalized) - len(new_list)
    rule_key_un = alert_monitor.make_rule_key(datastore_id, namespace, backup_type, backup_id)
    _audit("unignore_group", rule_key=rule_key_un, removed_count=removed_count)
    return jsonify({"ok": True, "removed": removed_count})


@app.route("/api/webui/info")
@require_auth
def webui_info():
    """Return web UI metadata (read-only flag, paths) for the frontend."""
    # Detect Docker environment
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("DOCKER_ENV") == "true"
    
    return jsonify({
        "read_only": WEBUI_READ_ONLY,
        "alerting_path": str(PROJECT_ROOT / "alerting"),
        "python_executable": sys.executable,
        "is_docker": is_docker,
    })


@app.route("/api/alerting/config")
@require_auth
def get_alerting_config():
    """Return the current alerting configuration with secrets redacted."""
    cfg = load_visual_alerting_config()
    return jsonify({"config": _redact_config(cfg), "read_only": WEBUI_READ_ONLY})


@app.route("/api/alerting/config", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("30 per minute")
def save_alerting_config():
    """Persist user-supplied changes to alerting/config.json."""
    guard = read_only_guard()
    if guard:
        return guard

    payload = request.get_json(silent=True) or {}
    raw_config = {}
    if ALERTING_CONFIG_PATH.exists():
        with open(ALERTING_CONFIG_PATH) as f:
            raw_config = json.load(f)

    for key in ("ntfy_url", "ntfy_topic"):
        if key in payload:
            raw_config[key] = str(payload[key])
    # ntfy_token: only update when the user submitted a real value, not the
    # redaction sentinel. Submitting an empty string clears the stored token.
    if "ntfy_token" in payload:
        submitted = str(payload["ntfy_token"])
        if submitted != _TOKEN_SENTINEL:
            raw_config["ntfy_token"] = submitted

    if "alert_cooldown_minutes" in payload:
        val = alert_monitor.coerce_int(payload["alert_cooldown_minutes"])
        if val is not None and val >= 0:
            raw_config["alert_cooldown_minutes"] = val

    if "daemon_interval_seconds" in payload:
        val = alert_monitor.coerce_int(payload["daemon_interval_seconds"])
        if val is not None and val >= 60:  # Minimum 1 minute
            raw_config["daemon_interval_seconds"] = val

    if "thresholds" in payload and isinstance(payload["thresholds"], dict):
        raw_thr = raw_config.setdefault("thresholds", {})
        for k in ("storage_warn_percent", "storage_crit_percent", "gc_max_age_hours", "verification_max_age_days"):
            if k in payload["thresholds"]:
                val = alert_monitor.coerce_int(payload["thresholds"][k])
                if val is not None and val > 0:
                    raw_thr[k] = val

    if "quiet_hours" in payload and isinstance(payload["quiet_hours"], dict):
        raw_qh = raw_config.setdefault("quiet_hours", {})
        qh = payload["quiet_hours"]
        if "enabled" in qh:
            raw_qh["enabled"] = bool(qh["enabled"])
        for str_key in ("start", "end"):
            if str_key in qh:
                raw_qh[str_key] = str(qh[str_key])
        if "min_priority" in qh:
            val = alert_monitor.coerce_int(qh["min_priority"])
            if val is not None:
                raw_qh["min_priority"] = val

    if "schedule_learning" in payload and isinstance(payload["schedule_learning"], dict):
        raw_sl = raw_config.setdefault("schedule_learning", {})
        sl = payload["schedule_learning"]
        if "enabled" in sl:
            raw_sl["enabled"] = bool(sl["enabled"])
        if "timezone" in sl:
            raw_sl["timezone"] = str(sl["timezone"])
        for int_key in ("history_window_days", "min_occurrences", "time_tolerance_minutes",
                        "due_grace_minutes", "stale_after_days", "snapshot_retention_count"):
            if int_key in sl:
                val = alert_monitor.coerce_int(sl[int_key])
                if val is not None and val > 0:
                    raw_sl[int_key] = val

    if "notification_priorities" in payload and isinstance(payload["notification_priorities"], dict):
        raw_np = raw_config.setdefault("notification_priorities", {})
        for sev in ("warning", "critical"):
            if sev in payload["notification_priorities"]:
                val = alert_monitor.coerce_int(payload["notification_priorities"][sev])
                if val is not None and 1 <= val <= 5:
                    raw_np[sev] = val

    with open(ALERTING_CONFIG_PATH, "w") as f:
        json.dump(raw_config, f, indent=2)

    changed_keys = [k for k in payload if k != "ntfy_token"]
    _audit("config_save", changed_keys=changed_keys)
    return jsonify({"ok": True})


@app.route("/api/alerting/test/dry-run", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("5 per minute")
def alerting_test_dry_run():
    """Simulate a full alerting run and return what would be sent — without sending."""
    alerting_config = load_visual_alerting_config()
    alerting_state, _ = load_visual_alerting_state()
    group_rules, _ = load_visual_group_rules()

    try:
        datastores = api_get("/monitoring/v1/datastores")
    except requests.RequestException as e:
        return jsonify({"error": f"API unreachable: {e}"}), 502

    quiet = alert_monitor.is_quiet_hours(alerting_config)
    results = []
    for ds in datastores:
        ds_id = ds["id"]
        try:
            detail = api_get(f"/monitoring/v1/datastores/{ds_id}")
        except requests.RequestException:
            detail = ds

        backup_inventory = None
        inventory_error = None
        if detail.get("metrics"):
            try:
                backup_inventory = alert_monitor.fetch_backup_inventory(alerting_config, ds_id)
            except (requests.RequestException, RuntimeError) as inv_err:
                inventory_error = str(inv_err)

        alerts, backup_status = alert_monitor.check_datastore(
            detail, alerting_config, alerting_state,
            backup_inventory=backup_inventory,
            group_rules=group_rules,
            persist_group_rules=False,
        )

        serialized = []
        for alert in alerts:
            would_send = True
            suppressed_by = None
            if quiet and alert.priority < alerting_config["quiet_hours"].get("min_priority", 4):
                would_send = False
                suppressed_by = "quiet_hours"
            elif not alert_monitor.should_alert(alerting_config, alerting_state, alert.key):
                would_send = False
                suppressed_by = "cooldown"
            serialized.append({
                "title": alert.title,
                "message": alert.message,
                "priority": alert.priority,
                "tags": alert.tags,
                "would_send": would_send,
                "suppressed_by": suppressed_by,
            })

        results.append({
            "datastore": detail.get("name", ds_id),
            "backup_status": backup_status,
            "inventory_error": inventory_error,
            "alerts": serialized,
        })

    total_alerts = sum(len(r["alerts"]) for r in results)
    would_send_count = sum(1 for r in results for a in r["alerts"] if a["would_send"])
    return jsonify({
        "datastores_checked": len(results),
        "total_alerts": total_alerts,
        "would_send": would_send_count,
        "quiet_hours_active": quiet,
        "results": results,
    })


@app.route("/api/alerting/test/live", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("5 per minute")
def alerting_test_live():
    """Run a real alerting check and send notifications via ntfy."""
    guard = read_only_guard()
    if guard:
        return guard

    config = alert_monitor.load_config()
    state = alert_monitor.load_state()
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            alert_monitor.run_check(config, state)
        _audit("test_live")
        return jsonify({"ok": True, "output": output.getvalue()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "output": output.getvalue()}), 500


@app.route("/api/alerting/test/notify", methods=["POST"])
@require_auth
@require_csrf
@limiter.limit("5 per minute")
def alerting_test_notify():
    """Send a single test notification to verify the ntfy configuration."""
    guard = read_only_guard()
    if guard:
        return guard

    body = request.get_json(silent=True) or {}
    severity = body.get("severity", "warning")
    if severity not in ("warning", "critical"):
        severity = "warning"

    config = alert_monitor.load_config()
    ntfy_url = config.get("ntfy_url", "").rstrip("/")
    ntfy_topic = config.get("ntfy_topic", "")
    ntfy_token = config.get("ntfy_token", "")

    if not ntfy_url or not ntfy_topic:
        return jsonify({"ok": False, "error": "ntfy_url and ntfy_topic must be configured."}), 400

    try:
        _validate_ntfy_url(ntfy_url)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    prio_cfg = config.get("notification_priorities", {})
    default_prio = 4 if severity == "warning" else 5
    priority = max(1, min(5, int(prio_cfg.get(severity) or default_prio)))

    url = f"{ntfy_url}/{ntfy_topic}"
    severity_label = severity.capitalize()
    tag = "warning" if severity == "warning" else "rotating_light"
    headers = {
        "Title": alert_monitor._ntfy_header_safe(f"PBS Monitor - Test Notification ({severity_label})"),
        "Priority": str(priority),
        "Tags": alert_monitor._ntfy_header_safe(tag),
    }
    if ntfy_token:
        headers["Authorization"] = f"Bearer {ntfy_token}"

    try:
        resp = requests.post(
            url,
            data=f"Test {severity_label} notification from PBS Monitor (ntfy priority {priority}).".encode("utf-8"),
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        alert_monitor.append_notification_log(ALERTING_LOG_PATH, {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "webui-test",
            "title": f"PBS Monitor - Test Notification ({severity_label})",
            "message": f"Test {severity_label} notification from PBS Monitor (ntfy priority {priority}).",
            "priority": priority,
            "datastore_name": None,
            "alert_key": None,
        })
        _audit("test_notify", severity=severity)
        return jsonify({"ok": True, "url": url, "priority": priority})
    except requests.RequestException as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        return jsonify({"ok": False, "error": str(e), "status_code": status_code}), 502


@app.route("/api/alerting/notification-log", methods=["GET"])
@require_auth
def get_notification_log():
    """Return the notification history log."""
    try:
        if ALERTING_LOG_PATH.exists():
            with open(ALERTING_LOG_PATH) as f:
                entries = json.load(f)
        else:
            entries = []
        return jsonify({"entries": entries, "count": len(entries)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/alerting/notification-log", methods=["DELETE"])
@require_auth
@require_csrf
@limiter.limit("30 per minute")
def clear_notification_log():
    """Clear the notification history log."""
    guard = read_only_guard()
    if guard:
        return guard
    try:
        with open(ALERTING_LOG_PATH, "w") as f:
            json.dump([], f)
        _audit("notification_log_clear")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/health")
@require_auth
def get_health():
    """Proxy the platform health check."""
    try:
        data = api_get_public("/health")
        return jsonify(data)
    except requests.RequestException as e:
        return jsonify({"status": "error", "message": str(e)}), 502


@app.route("/api/datastores/<datastore_id>/backups")
@require_auth
def get_datastore_backups(datastore_id):
    """Fetch namespace-aware backup browsing data for a single datastore."""
    base_path = f"/monitoring/v1/datastores/{datastore_id}/backups"

    try:
        overview = api_get(base_path)
    except requests.RequestException as e:
        return jsonify({"error": f"Backup browsing unavailable: {e}"}), 502

    namespace_entries = overview.get("namespaces") or [{"ns": "", "comment": None}]
    namespace_entries = sorted(
        namespace_entries,
        key=lambda item: (item.get("ns", "") != "", item.get("ns", "")),
    )

    namespaces = []
    for namespace_meta in namespace_entries:
        namespace_value = namespace_meta.get("ns", "")
        try:
            namespace_data = api_get(base_path, params={"ns": namespace_value})
        except requests.RequestException as e:
            namespaces.append({
                "ns": namespace_value,
                "label": namespace_value or "root",
                "comment": namespace_meta.get("comment"),
                "group_count": 0,
                "snapshot_count": 0,
                "groups": [],
                "error": str(e),
            })
            continue

        namespaces.append(normalize_namespace(namespace_meta, namespace_data))

    protocols = {}
    protocol_paths = {
        "rsync": f"/monitoring/v1/datastores/{datastore_id}/backups/rsync",
        "sftp": f"/monitoring/v1/datastores/{datastore_id}/backups/sftp",
        "zfs_recv": f"/monitoring/v1/datastores/{datastore_id}/backups/zfs-recv",
    }
    for protocol_name, protocol_path in protocol_paths.items():
        try:
            payload = api_get(protocol_path)
        except requests.RequestException:
            continue

        if protocol_name == "zfs_recv" and should_hide_zfs_recv(payload):
            continue

        if isinstance(payload, list) and payload:
            protocols[protocol_name] = payload
        elif isinstance(payload, dict) and payload:
            protocols[protocol_name] = payload

    return jsonify({
        "datastore_id": datastore_id,
        "summary": {
            "namespace_count": len(namespaces),
            "group_count": sum(namespace.get("group_count", 0) for namespace in namespaces),
            "snapshot_count": sum(namespace.get("snapshot_count", 0) for namespace in namespaces),
        },
        "namespaces": namespaces,
        "protocols": protocols,
    })


@app.route("/api/platform-stats")
@require_auth
def get_platform_stats():
    """Fetch public platform statistics."""
    stats = {}
    try:
        stats["storage"] = api_get_public("/public/total-storage")
    except requests.RequestException:
        stats["storage"] = None
    try:
        stats["backups_30d"] = api_get_public("/public/backups-30-days")
    except requests.RequestException:
        stats["backups_30d"] = None
    try:
        stats["traffic_30d"] = api_get_public("/public/traffic-30-days")
    except requests.RequestException:
        stats["traffic_30d"] = None
    return jsonify(stats)


if __name__ == "__main__":
    if FLASK_DEBUG:
        app.run(host=WEBUI_HOST, port=WEBUI_PORT, debug=True)
    else:
        from waitress import serve
        serve(app, host=WEBUI_HOST, port=WEBUI_PORT)
