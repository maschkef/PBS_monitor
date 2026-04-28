# Technical Documentation: alerting/schedule.py

## Overview
This module implements the core logic for schedule detection, evaluation, and generating "missed backup" alerts. It operates without file I/O, performing pure timezone-aware computation to analyze backup snapshot timelines against explicit user configurations or auto-learned historical patterns.

## Alerts Data Class
### `Alert`
A simple class defining a single alert containing datastore name, title, message, priority, tags, unique deduplication key, scope (e.g. `datastore` or `group`), and the related `group_rule_key`.

## Time and Formatting

### `parse_iso(iso_str)`
Parses an ISO 8601 string to a timezone-aware python `datetime` object. Handles `"Z"` as UTC (`+00:00`).

### `get_schedule_timezone(config)`
Retrieves the target Python `tzinfo` (from `zoneinfo`) for schedule calculations. Defaults to the system's local timezone if none is provided or if parsing fails.

### `format_interval_minutes(interval_minutes)`
Converts a raw minute value into a human-readable interval like "every 2h 30m" or "every 45m".

## Schedule Model Builders

### `build_schedule_model_from_rule(rule, fallback_timezone)`
Generates an active `schedule_model` payload based on a configured `rule`.
**Logic:** Distinguishes between `interval`, `daily`, and `weekly` rules. Depending on the `schedule_kind`, maps `interval_minutes` or daily/weekly `slots` array to a populated, standardized schedule dictionary reflecting the desired state. Uses the rule's specified timezone or the fallback.

### `schedule_model_has_definition(schedule_model)`
Returns `True` if a schedule model actually contains workable targets (e.g. valid slots or an active interval), and `False` otherwise.

### `refresh_schedule_summary(ds_state)`
Counts how many schedules were auto-learned and the total active slots for a specific datastore. Modifies the datastore state in-place to cache these aggregates for the frontend.

## Occurrence Transformation

### `snapshot_to_local_occurrence(snapshot, tzinfo)`
Converts a persisted snapshot dictionary into an enriched `occurrence` structure.
**Logic:** Computes local timezone `datetime`, `date`, `weekday`, and `minute_of_day` properties to facilitate efficient clustering.

### `hours_since(iso_str)`
Calculates the floating-point number of hours passed since the provided ISO string, based on the current UTC time. Returns `inf` on invalid strings.

## Due-Time Algorithms

### `cluster_day_occurrences(occurrences, tolerance_minutes)`
Groups historical daily occurrences if they happened within `tolerance_minutes` of each other.
**Logic:** Sorts occurrences by minute of day. Scans the list to group items within proximity, continually recalculating the median `minute_of_day` of each cluster as new items are inserted. If an occurrence falls outside the range of any cluster, a new cluster is birthed.

### `find_recent_due(slot, now_local)`
Calculates the exact `datetime` a specific weekly backup slot was last expected to occur, prior to `now_local`.

### `compute_anchor_aligned_due(anchor_minute, interval_minutes, now_local)`
Calculates the expected interval due time based on a fixed daily `anchor_minute` (e.g., backups run every 6 hours, anchored at 00:00). Computes offsets accurately matching the current interval block.

### `compute_next_expected_backup(group_state, schedule_model, tzinfo)`
Derives the immediate *future* time a backup should occur based on interval configs, or daily/weekly configured slots, predicting the next matching timestamp.

## Machine Learning & Detection

### `detect_interval_schedule(occurrences, config, now_local, tzinfo)`
Scans snapshot occurrences to deduce a recurring frequent backup window (less than `INTERVAL_MODEL_MAX_MINUTES` = 360).
**Logic:** Calculates gaps between chronological occurrences. Takes the median gap and evaluates if 75% or more of the gaps fall within `tolerance_minutes` of the median. If criteria is met, returns an `interval` model.

### `detect_daily_schedule(occurrences, config, tzinfo)`
Learns recurrent times of day irrespective of weekday.
**Logic:** Invokes `cluster_day_occurrences`. Clusters must contain hits from at least 4 distinct weekdays, verifying a true daily recurrence. Returns a populated `daily` model or `None` if rules are insufficient. Uses `stale_after_days` configuration to disable aged rules.

### `evaluate_schedule_model(group_state, config, tzinfo)`
The primary orchestrator for learning history models.
**Logic:** 
1. Trims history to `history_window_days`.
2. Converts recent snapshots into local occurrences.
3. Attempt interval detection; return if successful.
4. Attempt daily detection; return if successful.
5. Attempt weekly slot detection by breaking down occurrences per weekday and clustering.
Returns the highest fidelity `weekly`, `daily`, `interval`, or `none` model, tagged with `learning`, `learned`, or `stale` status based on history depth.

## Alerting Evaluators

### `build_missed_slot_alert(...)` & `build_missed_interval_alert(...)`
Helper functions returning a constructed `Alert` object formatted nicely to indicate which datastore/namespace missed their slot, logging current time and expectation.

### `evaluate_missed_backup_alerts(ds, group_state, schedule_model, config, tzinfo)`
Determines if an expected backup window has been skipped.
**Logic:** 
Iterates over the `schedule_model`.
- **For intervals:** Computes `due_dt` and checks if `now` exceeds the due target plus `due_grace_minutes`. Verifies if any historical occurrence overlaps this window. Raises `build_missed_interval_alert` if not.
- **For daily/weekly slots:** Identifies the expected historical `due_time`. Scans recent occurrences. If no occurrence overlaps `window_start` (due - tolerance) and `window_end` (due + grace), raises a `build_missed_slot_alert`. Provides context like off-schedule (manual) snapshots that happened the same day. Returns a list of generated alerts.
