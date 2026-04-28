# Technical Documentation: alerting/notification.py

## Overview
This module handles sending push notifications via the [ntfy](https://ntfy.sh) service. It also provides utility functions for formatting byte sizes, logging notifications to disk, guarding against Server-Side Request Forgery (SSRF) when resolving the ntfy endpoint, and enforcing alert cooldowns and quiet hours.

## Functions

### `format_bytes(b)`
- **Description:** Converts an integer byte value into a human-readable base-1000/SI unit string (e.g., KB, MB, GB, TB).
- **Logic:** Iterates over the units array, dividing the value by 1000 in each step until the absolute value is less than 1000. Returns `"0 B"` if the input is falsey. Maxes out at PB (Petabytes).

### `_ntfy_header_safe(value)`
- **Description:** Normalizes and sanitizes a string to contain only latin-1 safe characters, making it suitable for HTTP headers used by the ntfy API.
- **Logic:** Uses `unicodedata.normalize("NFKD", value)` to decompose characters, then encodes them to `latin-1` (ignoring errors) and decodes them back to a string.

### `append_notification_log(log_path, entry)`
- **Description:** Appends a new notification record to the local JSON log file, maintaining a maximum capacity (`_MAX_NOTIFICATION_LOG_ENTRIES` = 500).
- **Logic:** Attempts to read the existing JSON array from `log_path`. If it fails or the file doesn't exist, initializes an empty list. Appends the new `entry` to the list. Slices the list to keep only the last 500 entries, and then writes it back. All exceptions are caught and warned via `stderr`, ensuring alerting is never blocked by a failed disk write.

### `_validate_ntfy_url_monitor(url: str, allow_private: bool = True)`
- **Description:** Acts as an SSRF guard by checking if the configured ntfy server URL is safe.
- **Logic:** Parses the URL and ensures the scheme is `http` or `https` and a hostname is present. If `allow_private` is `False`, it resolves the hostname using `socket.getaddrinfo`. The resolved IP addresses are then checked against a predefined list of private/reserved IP networks (e.g., `127.0.0.0/8`, `10.0.0.0/8`, `192.168.0.0/16`). If a match is found, a `ValueError` is raised, rejecting the URL.

### `send_ntfy(config, alert)`
- **Description:** Dispatches a single alert message via a POST request to the configured ntfy topic.
- **Logic:** 
  1. Extracts `ntfy_topic` and `ntfy_url` from the provided `config` dictionary. Skips sending if either is missing.
  2. Constructs HTTP headers containing the `Title` (sanitized), `Priority`, and `Tags` of the alert.
  3. Checks for an authorization token (`NTFY_TOKEN` env var or `ntfy_token` config) and adds a Bearer header if present.
  4. Validates the ntfy URL using `_validate_ntfy_url_monitor`.
  5. URL-encodes the topic and performs a `requests.post` to the ntfy endpoint with the alert's message as UTF-8 encoded data.
  6. Returns `True` on success or `False` if any exceptions (`requests.RequestException` or SSRF violation) occur.

### `is_quiet_hours(config)`
- **Description:** Evaluates whether the current system time falls within the configured "quiet hours" interval.
- **Logic:** Retrieves the `quiet_hours` configuration object. If disabled, returns `False`. Gets the current time in `"HH:MM"` format. Handles both standard intervals (e.g., "08:00" to "17:00") and intervals spanning midnight (e.g., "22:00" to "07:00"). Returns `True` if the current time string falls within the bounds.

### `should_alert(config, state, alert_key)`
- **Description:** Determines if an alert should be suppressed because it was fired too recently (cooldown period).
- **Logic:** Looks up `alert_key` in the `last_alerts` dictionary of the provided `state`. If there's no record, returns `True`. Calculates the elapsed time in minutes between the last alert's timestamp (parsed using `alerting.schedule.parse_iso`) and the current UTC time. Returns `True` if the elapsed time is greater than or equal to the `alert_cooldown_minutes` config (defaults to 60).
