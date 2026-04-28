# Technical Documentation: webui/alerting_ui.py

## Overview
This module bridges the backend alerting engine (`alerting/monitor.py`) with the Flask frontend. It generates read-only "visual" alert objects and formatted schedule timelines so the Web UI dashboard can accurately reflect the same alerts and health metrics that would trigger a push notification, without actually sending them.

It explicitly operates outside of the Flask request context, taking paths and config dicts as arguments, which makes it highly testable.

## State Loaders

### `load_visual_alerting_config(config_path)`
Safely loads the alerting `config.json` without modifying the filesystem (unlike `monitor.load_config` which copies from an example if missing). Missing fields fall back to `monitor.DEFAULT_CONFIG`. Normalizes `ignored_groups` during load.

### `load_visual_alerting_state(state_path)`
Reads `state.json`. Returns the migrated state dict alongside a string flag (`"persisted"` or `"ephemeral"`) so the UI knows if it is displaying live disk data or generated defaults.

### `load_visual_group_rules()`
Reads `group_rules.json`. Returns migrated group rules alongside a string flag indicating `"persisted"` or `"ephemeral"`.

## Model Formatting

### `priority_to_health(priority)`
Translates a numeric priority (1-5) into a UI CSS class label:
- 4 or 5 -> `"critical"`
- 3 -> `"warning"`
- 1 or 2 -> `"healthy"`

### `serialize_schedule_model(schedule_model)`
Converts the dense backend schedule model into a predictable, flat dictionary tailored for Jinja2 template iteration in the UI.

## Group Aggregation

### `collect_schedule_groups(ds_state, tzinfo)`
Transforms the raw `backup_groups` dict from datastore state into a sorted array of backup-group metadata ready for the "Schedules" tab in the frontend.
**Logic:**
1. Scans `ds_state` for active alerts specifically tagged with `scope="group"`.
2. Loops through all backup groups. Extracts their configured, learned, and effective schedule models.
3. Invokes `monitor.compute_next_expected_backup` to project the next backup time.
4. Bundles rule lock status, display names, and rule keys.
5. Sorts the resulting list so that groups with active alerts appear first, followed by locked (manually configured) groups, and then alphabetically by namespace and label.

## Visual Alert Engine

### `build_visual_alerting(detail, alerting_config, alerting_state, group_rules, rules_source, state_path, fetch_inventory)`
The primary orchestrator for UI datastore health.
**Logic:**
1. If `fetch_inventory` is `True` and metrics are available, it makes live API calls to PBS to pull fresh snapshot lists via `monitor.fetch_backup_inventory`.
2. Passes the live inventory (or `None`) into `monitor.check_datastore` to execute the exact same alert threshold logic as the background daemon, but ensuring `persist_group_rules=False` so the dashboard view is read-only.
3. Translates the returned `Alert` class instances into flat dictionaries.
4. Identifies the highest priority alert to set the overall datastore `health` status.
5. Augments the datastore's `ignored_groups` configuration with friendly display names matched from the current state.
6. Packs the results, alongside the output of `collect_schedule_groups`, into a massive dictionary that `app.py` passes directly to `dashboard.html`.
