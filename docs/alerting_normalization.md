# Technical Documentation: alerting/normalization.py

## Overview
This module contains pure, stateless helper functions for data normalization, schema migration, and generating default state objects. It interacts only with standard libraries and is crucial for keeping data consistent between the monitor backend and the persisted JSON files.

## Basic Converters

### `coerce_int(value)`
Attempts to cast the provided `value` to an `int`. Returns `None` if a `TypeError` or `ValueError` occurs.

### `unix_to_iso(timestamp)`
Converts a UNIX timestamp (via `coerce_int`) to an ISO 8601 formatted UTC string. Returns `None` if the input is invalid.

### `format_schedule_time(minute_of_day)`
Converts an integer `minute_of_day` (0-1439) into a zero-padded `"HH:MM"` string.

### `weekday_name(index)`
Returns a 3-letter abbreviation of the weekday given a 0-based index (0 = Mon, 6 = Sun).

## Key Generators

### `make_rule_key(datastore_id, namespace, backup_type, backup_id)`
Constructs a stable, predictable composite string key for persisting specific backup group rules.
**Logic:** Uses a compact JSON array format (`["ds","ns","type","id"]`) to guarantee key uniqueness across different backup group dimensions.

## Normalization Functions

### `normalize_weekly_slots(slots)`
Sorts and cleans up a list of weekly schedule slots.
**Logic:** Discards malformed dicts or out-of-bounds `weekday` (0-6) and `minute_of_day` (0-1439) values. Removes exact duplicates using a set for uniqueness. Returns a sorted list augmented with `weekday_name` and `time` string representations.

### `normalize_daily_slots(slots)`
Sorts and cleans up a list of daily schedule slots.
**Logic:** Similar to weekly slots, but only considers and validates `minute_of_day`. Sorts chronologically.

### `normalize_group_rule(raw_rule)`
Sanitizes a persisted backup group rule, applying defaults and validations.
**Logic:** Verifies the `schedule_kind` ("daily", "weekly", "interval", "none"). Validates and cleans `interval_minutes` and `interval_anchor_minute`. Standardizes boolean values and timezone string. Applies slot normalization functions.

### `normalize_ignored_group(raw_group)`
Normalizes an individual item from the "ignored groups" configuration.
**Logic:** Casts expected keys (`datastore_id`, `namespace`, `backup_type`, `backup_id`, `display_name`) to strings. Discards rules where all defining match keys are `None`.

### `normalize_ignored_groups(raw_groups)`
Iterates over a list of ignored groups and runs `normalize_ignored_group`, returning a sanitized list of valid selectors.

### `is_group_ignored(config, datastore_id, namespace, backup_type, backup_id)`
Checks if a specific backup group falls under any ignore rule.
**Logic:** Compares the arguments against each ignored group selector defined in the `config`. Returns `True` on the first match (if a selector property is not `None` and exactly matches the argument). Returns `False` otherwise.

## Default State Constructors

### `empty_inventory_summary()`
Returns a zero-initialized dict for `namespace_count`, `group_count`, and `snapshot_count`.

### `default_datastore_state(name="unknown")`
Initializes a new structure for a single datastore, including an empty inventory, zeroed schedule summary, and empty `backup_groups` dict.

### `default_state()`
Constructs the root state dictionary with the current `_STATE_VERSION` (2) and empty `datastores` and `last_alerts`.

### `default_group_rules()`
Constructs the default group rules dictionary with the current `_GROUP_RULES_VERSION` (1) and empty `groups`.

## Snapshot History Helpers

### `normalize_snapshot_entries(entries, limit=None)`
Sanitizes a list of snapshot records.
**Logic:** Casts fields, ensuring `backup_time` and `size` are valid ints. Returns a list sorted in descending order by `backup_time`. Applies an optional upper `limit` bounds.

### `merge_snapshot_histories(existing_entries, new_entries, limit)`
Merges two sets of snapshot histories.
**Logic:** Normalizes both lists and inserts them into a dictionary keyed by `backup_time` to automatically deduplicate entries. The latest entry wins. Returns a sorted and trimmed list based on `limit`.

## Schema Migration Functions

### `migrate_inventory_summary(summary)`
Safely converts an old inventory dict, coercing its fields to zero-fallback integers.

### `migrate_backup_group_state(raw_group)`
Upgrades a single backup group dictionary to the current required schema.
**Logic:** Populates current and observed snapshots, respecting capacity constants `_MAX_CURRENT_SNAPSHOT_DETAILS` (24) and `_MAX_OBSERVED_SNAPSHOT_HISTORY` (1000). Resolves ambiguous snapshot counts and defaults schedule models if absent.

### `migrate_state(raw_state)`
Upgrades the root `state.json` payload from older structures to `_STATE_VERSION = 2`.
**Logic:** Iterates datastores, upgrading their internal summaries, group summaries, and recursively migrating `backup_group` items.

### `migrate_group_rules(raw_rules)`
Upgrades the root `group_rules.json` payload to `_GROUP_RULES_VERSION = 1` by running `normalize_group_rule` over all keys.
