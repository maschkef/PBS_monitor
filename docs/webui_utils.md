# Technical Documentation: webui/normalizers.py & webui/validators.py

## Overview
These two modules provide stateless utilities for the Flask Web UI. They are strictly responsible for preparing backend data for safe frontend consumption (normalizers) and validating raw frontend input before writing to disk (validators).

---

## 1. webui/normalizers.py

This module prepares PBS backup metadata for the frontend templates, converting complex JSON payloads into view-friendly dictionaries.

### Byte Formatters
- **`format_bytes(b)`**: Formats an integer to human-readable SI units (KB, MB, GB, etc. base-1000). Primarily used for datastore storage metrics.
- **`format_binary_bytes(b)`**: Formats an integer to human-readable IEC units (KiB, MiB, GiB, etc. base-1024). Used exclusively for the Backup Browser interface to match PBS web UI standards.

### `unix_to_iso(timestamp)`
Converts a UNIX timestamp integer to an ISO 8601 UTC string.

### Browser Normalization Pipeline
These functions incrementally build the data structure required by the Web UI Backup Browser.

- **`normalize_backup_file(file_entry)`**: Maps a raw backup file entry, converting the `size` to `size_human` and extracting the `csum` (checksum) and `filename`.
- **`normalize_backup_snapshot(snapshot)`**: Converts a single snapshot. Maps the `backup_time`, applies `format_binary_bytes` to the snapshot size, lists nested files, and resolves the `verification_state` (error, ok, etc.).
- **`is_trivial_zfs_recv_entry(entry)`** & **`should_hide_zfs_recv(payload)`**: Heuristics to hide internal, zero-byte ZFS replication snapshots from the UI to reduce noise.
- **`normalize_backup_group(group, snapshots)`**: Aggregates a list of normalized snapshots under a backup group. Computes `latest_comment`, gathers `distinct_comments`, and calculates total backup counts.
- **`normalize_namespace(namespace_meta, namespace_data)`**: The top-level normalizer for the browser. Groups snapshots by their `backup_type` and `backup_id`. Returns a structured dictionary containing a list of `groups`, with metrics like `snapshot_count` pre-calculated.

### Time Formatters
- **`time_ago(iso_str)`**: Converts an ISO timestamp into a relative "Xm ago", "Xh ago", or "Xd ago" string. If the timestamp is in the future, it delegates to `format_time_until`.
- **`time_until(iso_str)`** / **`format_time_until(seconds)`**: Converts an ISO timestamp into a relative "in Xm", "in Xh" string for scheduled events.

---

## 2. webui/validators.py

This module contains pure functions to validate HTTP JSON payloads received from the UI. They all raise `ValueError` on invalid input.

### Constants
- **`_TOKEN_SENTINEL` (`"***CONFIGURED***"`)**: A masked string sent to the frontend to represent an existing secret (like `ntfy_token`). It prevents exposing secrets.
- **`_TIME_RE`**: A strict regex validating 24-hour time formats (`HH:MM`).
- **`_CONFIG_STR_MAX`** & **`_RULE_STR_MAX`**: Dictionaries defining strict length limits for API payload fields to prevent DoS via large inputs.

### `_validate_ntfy_url(url)`
Parses the user-provided `ntfy_url`. Ensures that the URL has a valid HTTP/HTTPS scheme and a hostname.

### `_redact_config(cfg, ntfy_token_override)`
Takes a live configuration dictionary and replaces `ntfy_token` with `_TOKEN_SENTINEL`. Sets a boolean `ntfy_token_set` to help the frontend render appropriately without leaking the token itself.

### Payload Validators
These functions validate incoming JSON requests against expected schemas and bounds.
- **`_validate_config_payload(payload, coerce_int_fn)`**: Exhaustively checks the `config.json` payload. Validates string lengths, bounds checks integers (like `daemon_interval_seconds >= 60`), validates nested structures (`thresholds`, `quiet_hours`, `schedule_learning`, `notification_priorities`), and verifies regex for time values.
- **`_validate_group_rule_payload(payload)`**: Validates group rule payload constraints (datastore_id, namespace, timezone lengths) against `_RULE_STR_MAX`.
- **`_validate_ignore_group_payload(payload)`**: Validates payload parameters when a user clicks "Ignore" on a backup group in the UI.
