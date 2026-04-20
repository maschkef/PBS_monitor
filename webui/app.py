#!/usr/bin/env python3
"""PBS Monitor Web UI — Ad-hoc status dashboard for remote-backups.com datastores."""

import contextlib
import io
import json
import logging
import os
import secrets
import sys
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alerting import monitor as alert_monitor  # noqa: E402
from webui import normalizers as _normalizers  # noqa: E402
from webui import validators as _validators  # noqa: E402
from webui import alerting_ui as _alerting_ui  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

app = Flask(__name__, static_folder="static")

# When running behind a reverse proxy (e.g. Traefik), wrap the app with
# ProxyFix so that request.remote_addr reflects the real client IP from
# X-Forwarded-For instead of the proxy's Docker-internal address.
# This makes both rate-limiting and audit logging work correctly.
if WEBUI_PROXY_COUNT > 0:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=WEBUI_PROXY_COUNT, x_proto=WEBUI_PROXY_COUNT, x_host=WEBUI_PROXY_COUNT)

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
# When set, overrides ntfy_token from config.json without ever writing the
# secret to disk.  Follows the same pattern as WEBUI_SECRET_KEY.
NTFY_TOKEN = os.environ.get("NTFY_TOKEN", "").strip()
# Set to 1 when the app is served via HTTPS (Traefik/reverse-proxy with TLS).
# Enforces the Secure flag on the session cookie so it is never sent over HTTP.
WEBUI_SECURE_COOKIES = os.environ.get("WEBUI_SECURE_COOKIES", "").lower() in ("1", "true", "yes")
# Set to 1 to omit absolute server paths from /api/webui/info responses.
WEBUI_HIDE_SERVER_PATHS = os.environ.get("WEBUI_HIDE_SERVER_PATHS", "").lower() in ("1", "true", "yes")
# Number of reverse-proxy hops in front of Flask (e.g. 1 for a single Traefik
# instance).  When non-zero, Flask reads the real client IP from the
# X-Forwarded-For header rather than using the Docker-internal proxy address.
# Only set this when you control the proxy and trust its headers.
try:
    WEBUI_PROXY_COUNT = int(os.environ.get("WEBUI_PROXY_COUNT", "0"))
except ValueError:
    WEBUI_PROXY_COUNT = 0


# ── Re-exports for backward compatibility (tests import these from webapp) ─────
# Sentinel and pure helpers live in validators.py; expose them here so that
# existing tests referencing ``webapp._TOKEN_SENTINEL`` etc. keep working.
_TOKEN_SENTINEL = _validators._TOKEN_SENTINEL
_validate_ntfy_url = _validators._validate_ntfy_url
_validate_group_rule_payload = _validators._validate_group_rule_payload


def _redact_config(cfg: dict) -> dict:
    """Return a copy of cfg safe for browser consumption (secrets replaced with sentinel)."""
    return _validators._redact_config(cfg, ntfy_token_override=NTFY_TOKEN)


def _validate_config_payload(payload: dict) -> None:
    """Validate a user-supplied alerting config payload; raises ValueError on bad input."""
    _validators._validate_config_payload(payload, alert_monitor.coerce_int)


# ── Secret key configuration ──────────────────────────────────────────────────

def _configure_secret_key() -> str:
    """Return the Flask secret key from environment, or a volatile fallback."""
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
app.config["SESSION_COOKIE_SECURE"] = WEBUI_SECURE_COOKIES


@app.after_request
def add_security_headers(response):
    """Inject security headers on every response."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "
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


# ── Auth helpers ──────────────────────────────────────────────────────────────

def auth_enabled() -> bool:
    """Return True when password-based authentication is configured."""
    return bool(WEBUI_PASSWORD)


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
    """Decorator: validate X-CSRF-Token header for all state-changing routes."""
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


# ── Thin wrappers around alerting_ui helpers (inject module-level paths) ──────

def load_visual_alerting_config() -> dict:
    return _alerting_ui.load_visual_alerting_config(ALERTING_CONFIG_PATH)


def load_visual_alerting_state() -> tuple:
    return _alerting_ui.load_visual_alerting_state(ALERTING_STATE_PATH)


def load_visual_group_rules() -> tuple:
    return _alerting_ui.load_visual_group_rules()


def build_visual_alerting(detail, alerting_config, alerting_state, group_rules, rules_source, *, fetch_inventory=True):
    return _alerting_ui.build_visual_alerting(
        detail, alerting_config, alerting_state, group_rules, rules_source,
        ALERTING_STATE_PATH, fetch_inventory=fetch_inventory,
    )


# ── Convenience aliases for normalizer functions used directly in routes ───────
format_bytes = _normalizers.format_bytes
time_ago = _normalizers.time_ago
time_until = _normalizers.time_until
normalize_namespace = _normalizers.normalize_namespace
should_hide_zfs_recv = _normalizers.should_hide_zfs_recv


# ── Misc route helpers ────────────────────────────────────────────────────────

def read_only_guard():
    """Return a 403 JSON response when WEBUI_READ_ONLY is active, else None."""
    if WEBUI_READ_ONLY:
        return jsonify({"error": "Dashboard befindet sich im Read-Only-Modus."}), 403
    return None


def _write_json_atomic(path: Path, data) -> None:
    """Atomically write *data* as JSON to *path* via a same-directory temp file."""
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


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


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute; 20 per hour")
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
            app.logger.info("LOGIN SUCCESS  ip=%s", request.remote_addr)
            return redirect(url_for("index"))
        _audit("login_failure")
        app.logger.warning("LOGIN FAILURE  ip=%s", request.remote_addr)
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
    if rescale_range not in {"7d", "30d", "90d", "180d", "365d"}:
        return jsonify({"error": "Invalid rescale_range. Allowed: 7d, 30d, 90d, 180d, 365d."}), 400
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
            rescale_log = api_get(
                f"/monitoring/v1/datastores/{ds_id}/rescale-log",
                params={"range": rescale_range},
            )
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

    try:
        _validate_group_rule_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

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
    try:
        _validators._validate_ignore_group_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
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
    _write_json_atomic(ALERTING_CONFIG_PATH, raw_config)

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
    try:
        _validators._validate_ignore_group_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
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
    _write_json_atomic(ALERTING_CONFIG_PATH, raw_config)

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

    info = {
        "read_only": WEBUI_READ_ONLY,
        "is_docker": is_docker,
    }
    if not WEBUI_HIDE_SERVER_PATHS:
        info["alerting_path"] = str(PROJECT_ROOT / "alerting")
        info["python_executable"] = sys.executable
    return jsonify(info)


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

    try:
        _validate_config_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

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

    if "ntfy_allow_private_url" in payload:
        raw_config["ntfy_allow_private_url"] = bool(payload["ntfy_allow_private_url"])

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

    _write_json_atomic(ALERTING_CONFIG_PATH, raw_config)

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
        out = output.getvalue()
        return jsonify({"ok": True, "output": out[-8192:] if len(out) > 8192 else out})
    except Exception as e:
        out = output.getvalue()
        return jsonify({"ok": False, "error": str(e), "output": out[-8192:] if len(out) > 8192 else out}), 500


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
    ntfy_token = NTFY_TOKEN or config.get("ntfy_token", "")

    if not ntfy_url or not ntfy_topic:
        return jsonify({"ok": False, "error": "ntfy_url and ntfy_topic must be configured."}), 400

    try:
        _validate_ntfy_url(ntfy_url, allow_private=bool(config.get("ntfy_allow_private_url", False)))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    prio_cfg = config.get("notification_priorities", {})
    default_prio = 4 if severity == "warning" else 5
    priority = max(1, min(5, int(prio_cfg.get(severity) or default_prio)))

    url = f"{ntfy_url}/{quote(ntfy_topic, safe='')}"
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
        _write_json_atomic(ALERTING_LOG_PATH, [])
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
