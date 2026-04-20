"""Input validation and secret-redaction helpers for the PBS Monitor Web UI.

All functions here are pure (no Flask context required) and raise ``ValueError``
on invalid input so callers can return a clean 400 response.
"""

import ipaddress
import re
import socket
from urllib.parse import urlparse


# Sentinel returned to the browser instead of actual secret values.
# The frontend must submit this exact string back for the server to recognise
# that the user did not change the secret (i.e., preserve the stored value).
_TOKEN_SENTINEL = "***CONFIGURED***"

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

# HH:MM — strict: hours 00-23, minutes 00-59
_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

# Max lengths for free-text string fields in the alerting config payload.
_CONFIG_STR_MAX: dict[str, int] = {
    "ntfy_url": 2048,
    "ntfy_topic": 256,
    "ntfy_token": 512,
}

# Max lengths for string fields in the group-rule payload.
_RULE_STR_MAX: dict[str, int] = {
    "datastore_id": 128,
    "namespace": 128,
    "backup_type": 32,
    "backup_id": 256,
    "display_name": 256,
    "timezone": 64,
}


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


def _redact_config(cfg: dict, ntfy_token_override: str = "") -> dict:
    """Return a copy of cfg safe for browser consumption.

    Secrets are replaced with a sentinel so the frontend can distinguish
    'token is configured' from 'token is empty' without the actual value
    ever leaving the server.

    Pass ``ntfy_token_override`` when an env-var token takes precedence over
    the value stored in config.json (e.g. the ``NTFY_TOKEN`` env var).
    """
    redacted = dict(cfg)
    effective_token = ntfy_token_override or redacted.get("ntfy_token", "")
    redacted["ntfy_token_set"] = bool(effective_token)
    if effective_token:
        redacted["ntfy_token"] = _TOKEN_SENTINEL
    return redacted


def _validate_config_payload(payload: dict, coerce_int_fn) -> None:
    """Validate a user-supplied alerting config payload.

    Raises ValueError with a human-readable message for any invalid field so
    the route can return a 400 response without writing anything to disk.

    ``coerce_int_fn`` is passed in to avoid a hard import of alerting.monitor
    from this module (keeps the dependency explicit at the call site).
    """
    for key, maxlen in _CONFIG_STR_MAX.items():
        if key in payload:
            val = payload[key]
            if not isinstance(val, str):
                raise ValueError(f"{key} must be a string.")
            if len(val) > maxlen:
                raise ValueError(f"{key} must not exceed {maxlen} characters.")

    if "alert_cooldown_minutes" in payload:
        v = coerce_int_fn(payload["alert_cooldown_minutes"])
        if v is None or v < 0:
            raise ValueError("alert_cooldown_minutes must be a non-negative integer.")

    if "daemon_interval_seconds" in payload:
        v = coerce_int_fn(payload["daemon_interval_seconds"])
        if v is None or v < 60:
            raise ValueError("daemon_interval_seconds must be at least 60.")

    if "thresholds" in payload:
        if not isinstance(payload["thresholds"], dict):
            raise ValueError("thresholds must be an object.")
        thr = payload["thresholds"]
        for k in ("storage_warn_percent", "storage_crit_percent"):
            if k in thr:
                v = coerce_int_fn(thr[k])
                if v is None or not (1 <= v <= 100):
                    raise ValueError(f"thresholds.{k} must be between 1 and 100.")
        for k in ("gc_max_age_hours", "verification_max_age_days"):
            if k in thr:
                v = coerce_int_fn(thr[k])
                if v is None or v <= 0:
                    raise ValueError(f"thresholds.{k} must be a positive integer.")

    if "quiet_hours" in payload:
        if not isinstance(payload["quiet_hours"], dict):
            raise ValueError("quiet_hours must be an object.")
        qh = payload["quiet_hours"]
        for time_key in ("start", "end"):
            if time_key in qh:
                if not isinstance(qh[time_key], str) or not _TIME_RE.match(qh[time_key]):
                    raise ValueError(f"quiet_hours.{time_key} must be in HH:MM format (00:00–23:59).")
        if "min_priority" in qh:
            v = coerce_int_fn(qh["min_priority"])
            if v is None or not (1 <= v <= 5):
                raise ValueError("quiet_hours.min_priority must be between 1 and 5.")

    if "schedule_learning" in payload:
        if not isinstance(payload["schedule_learning"], dict):
            raise ValueError("schedule_learning must be an object.")
        sl = payload["schedule_learning"]
        if "timezone" in sl:
            if not isinstance(sl["timezone"], str) or len(sl["timezone"]) > 64:
                raise ValueError("schedule_learning.timezone must be a string of at most 64 characters.")
        for int_key in ("history_window_days", "min_occurrences", "time_tolerance_minutes",
                        "due_grace_minutes", "stale_after_days", "snapshot_retention_count"):
            if int_key in sl:
                v = coerce_int_fn(sl[int_key])
                if v is None or v <= 0:
                    raise ValueError(f"schedule_learning.{int_key} must be a positive integer.")

    if "notification_priorities" in payload:
        if not isinstance(payload["notification_priorities"], dict):
            raise ValueError("notification_priorities must be an object.")
        np_ = payload["notification_priorities"]
        for sev in ("warning", "critical"):
            if sev in np_:
                v = coerce_int_fn(np_[sev])
                if v is None or not (1 <= v <= 5):
                    raise ValueError(f"notification_priorities.{sev} must be between 1 and 5.")


def _validate_group_rule_payload(payload: dict) -> None:
    """Validate a user-supplied group-rule payload.

    Raises ValueError with a human-readable message for any invalid field.
    """
    for key, maxlen in _RULE_STR_MAX.items():
        val = payload.get(key)
        if val is not None:
            if not isinstance(val, str):
                raise ValueError(f"{key} must be a string.")
            if len(val) > maxlen:
                raise ValueError(f"{key} must not exceed {maxlen} characters.")
