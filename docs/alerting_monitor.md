# Technical Documentation: alerting/monitor.py

## Overview
This is the primary daemon/script that orchestrates the entire backend monitoring process. It connects to the `remote-backups.com` API, fetches the current state of Proxmox Backup Server (PBS) datastores, evaluates them against configuration thresholds (storage space, GC, verification) and historical backup schedules, and dispatches `ntfy` alerts upon discovering anomalies.

It heavily imports from `normalization.py`, `schedule.py`, and `notification.py` to maintain a stateless core logic flow, mapping only API payloads and disk I/O in this file.

## Core Setup & Configuration

### File I/O Management
The script determines `DATA_DIR` either via the `ALERTING_DATA_DIR` environment variable (for Docker mounts) or defaults to the script's directory. It manages three key JSON files:
- `config.json`: User configurations (API Base, Thresholds, ntfy).
- `state.json`: Persistent monitoring state (inventory history, snapshot data).
- `group_rules.json`: Explicit, user-defined schedules or rules for specific backup groups.

### `load_config()`, `load_state()`, `load_group_rules()`
Reads their respective JSON files. If missing, config creates a default from `config.json.example`, while state and rules return empty default structures. Calls the respective `migrate_*` functions from `normalization.py` to transparently upgrade legacy formats.

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
3. Automatically learns or evaluates backup schedules by delegating to `evaluate_schedule_model`.
4. Evaluates missed backups via `evaluate_missed_backup_alerts`.
5. Updates and normalizes the live `ds_state` (updating `current_snapshots` and `observed_snapshots` history rings).
6. Determines if explicitly defined `group_rules` changed and need saving.

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
7. Dispatches the remaining alerts via `send_ntfy` and appends them to the notification log.
8. Commits the updated `state.json`.

### `main()`
Parses CLI arguments. If `--daemon <seconds>` is provided, sets up `SIGINT`/`SIGTERM` handlers and runs `run_check` in an infinite loop punctuated by `time.sleep`. Otherwise, runs `run_check` exactly once.
