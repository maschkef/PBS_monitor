# Technical Documentation: alerting/monitor.py

## Overview
This is the primary daemon/script that orchestrates the entire backend monitoring process. It connects to the `remote-backups.com` API, fetches the current state of Proxmox Backup Server (PBS) datastores, evaluates them against configuration thresholds (storage space, GC, verification) and historical backup schedules, and dispatches `ntfy` alerts upon discovering anomalies.

It imports from `normalization.py`, `schedule.py`, and `notification.py` to keep the core schedule, migration, and notification logic in focused modules while this file handles API payloads, orchestration, and disk I/O.

## Core Setup & Configuration

### `ALERT_TITLE_TO_TYPE_KEY`
A module-level dict that maps every alert title string (e.g. `"GC Failed"`, `"Verification Overdue"`) to a stable config key (e.g. `"gc_failed"`, `"verification_overdue"`). These keys are what `notification_priorities.per_alert` uses in `config.json`, and are also exposed in the Web UI settings under **Per-Alert Priority Overrides**.

### `_resolve_alert_priority(alert, prio_cfg)`
Determines the final ntfy priority for a single alert object:
1. Looks up the alert's title in `ALERT_TITLE_TO_TYPE_KEY` to get its type key.
2. If `prio_cfg["per_alert"]` contains a non-null value for that key, clamps it to 1–5 and returns it immediately.
3. Otherwise falls back to the two-tier mapping: base priority ≥ 5 → `prio_cfg["critical"]` (default 5), base priority ≥ 4 → `prio_cfg["warning"]` (default 4), base priority 3 → unchanged.

### File I/O Management
The script determines `DATA_DIR` either via the `ALERTING_DATA_DIR` environment variable (for Docker mounts) or defaults to the script's directory. It manages these JSON files:
- `config.json`: User configuration (API base, thresholds, ntfy, heartbeat URL, daemon interval, quiet hours, ignored groups).
- `state.json`: Persistent monitoring state (inventory history, snapshot data).
- `group_rules.json`: Explicit, user-defined schedules or rules for specific backup groups.
- `notification_log.json`: Rolling history of sent alerts and test notifications.

### `load_config()`, `load_state()`, `load_group_rules()`
Reads their respective JSON files. If missing, config creates a default from `config.json.example`, while state and rules return empty default structures.
- `load_config()` uses an `auto_migrate_config` helper to automatically merge missing keys from `config.json.example` into the user's `config.json` without breaking existing settings.
- `load_state()` tracks the `last_run_version` (compared against the `VERSION` file) and emits console update notifications when version changes are detected.
Calls the respective `migrate_*` functions from `normalization.py` to transparently upgrade legacy formats.

### `save_state(state)`, `save_group_rules(group_rules)`
Atomic write operations to disk using `.tmp` files and `os.replace` to prevent data corruption during power loss or unexpected termination.

## API Client

### `api_get(config, path, params=None)`
A standard wrapper around `requests.get` to inject the `API_KEY` Bearer token into the `Authorization` header.

### `fetch_datastores(config)`
Hits `/monitoring/v1/datastores` to list all available datastores and iteratively fetches their details (e.g. metrics, gc, verification) via `/monitoring/v1/datastores/{id}`.

### `fetch_backup_inventory(config, datastore_id)`
Fetches the complete backup inventory for a specific datastore. It groups the results by namespaces. This data forms the baseline for snapshot and schedule tracking.

## Inventory Normalization Helpers
These functions map the raw API responses into structured datastore and backup group dicts.

### `extract_namespace_backup_groups(...)`
Groups raw snapshots belonging to the same namespace by their `backup_type` and `backup_id`. Returns a list of standardized group records using `build_backup_group_record`.

## Datastore State Management & Analysis

### `apply_backup_inventory_state(ds, ds_state, backup_inventory, config, group_rules, persist_group_rules)`
The most critical data-fusion function. It compares the newly fetched `backup_inventory` against the persisted `ds_state` (historical baseline).
**Key Logic:**
1. Triggers snapshot extraction logic.
2. Identifies if snapshots unexpectedly disappeared (e.g., dropped below `keep_last` limits). Generates `Snapshots Unexpectedly Removed` or `All Backups Gone` alerts.
3. Skips ignored groups while processing live inventory and purges ignored groups from persisted state so muted groups do not continue to drive learned schedules.
4. Updates and normalizes the live `ds_state` (updating `current_snapshots` and `observed_snapshots` history rings).
5. Automatically learns or evaluates backup schedules by delegating to `evaluate_schedule_model`.
6. Evaluates missed backups via `evaluate_missed_backup_alerts`.
7. Determines if explicitly defined `group_rules` changed and need saving.

### `check_datastore(ds, config, state, backup_inventory, group_rules, persist_group_rules)`
Evaluates high-level datastore health:
- Validates if metrics are present (alerts `Host Offline` if not).
- Checks storage capacity against `storage_warn_percent` (priority 3) and `storage_crit_percent` (priority 5).
- Checks Garbage Collection status and age (`gc_max_age_hours`). Alerts on failure, never ran, or overdue.
- Checks Verification status and age (`verification_max_age_days`). Alerts on failure, never ran, or overdue.
- Checks if "Immutable backups" have a pending disable request (security risk).
- Checks replication sync latency.
- Merges the datastore-level alerts with the backup-level alerts from `apply_backup_inventory_state`.

## Execution Flow

### `run_check(config, state)`
The master execution cycle run per interval.
1. Probes the remote-backups API `/health` endpoint.
2. Fetches datastores via `fetch_datastores`.
3. Loops through each datastore, pulling inventory and executing `check_datastore`.
4. Gathers all generated `Alert` objects.
5. Filters out alerts lower than the minimum priority if `is_quiet_hours` is active.
6. Filters out recently fired alerts using `should_alert` (cooldown period).
7. Resolves the final ntfy priority for each alert via `_resolve_alert_priority` (checks per-alert overrides, then tier defaults), then dispatches via `send_ntfy` and appends to the notification log. Catches any `NtfyDeliveryError` and sets an internal `ntfy_delivery_failed` flag.
8. Handles heartbeat pings with the following logic:
   - **Normal path** (`ntfy_delivery_failed = False`): pings `heartbeat_fail_url` when alerts were detected, `heartbeat_url` when none were. If only `heartbeat_url` is configured, no ping is sent when alerts exist — the timeout signals the problem. A failed ping generates an internal urgent alert sent via ntfy.
   - **ntfy delivery failed path** (`ntfy_delivery_failed = True`): pings `heartbeat_fail_url` if configured, so an external monitor receives an active failure signal even when push delivery is broken. `heartbeat_url` is never pinged in this path — doing so would either clear an alarm set by the fail URL, or (when only `heartbeat_url` is set) prevent the timeout from triggering the alert. Logs a `"Ntfy Delivery Failed"` entry to the notification log when any heartbeat URL is configured.
9. Commits the updated `state.json`.

### `main()`
Parses CLI arguments. If `--daemon <seconds>` is provided with a positive value, that value is used as the loop interval. If `--daemon` is provided without a value or as `--daemon 0`, the loop interval is read from `daemon_interval_seconds` in config and falls back to the built-in default. In daemon mode, `config.json` is reloaded before every check so Web UI changes to thresholds, ignored groups, notification settings, and the daemon interval apply to the next loop. Otherwise, runs `run_check` exactly once.
