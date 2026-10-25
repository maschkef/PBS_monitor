# Technical Documentation: PBS Monitor Architecture Overview

## Introduction
The PBS Monitor is an open-source (MIT), dependency-lean hobby project. It is designed to track the health of Proxmox Backup Server (PBS) datastores via the `remote-backups.com` monitoring API. 

The software stack relies on Python 3.11+, Flask, Flask-Limiter, Waitress, python-dotenv, Requests, HTML/Vanilla JS/CSS, and `ntfy.sh` for push notifications.

## Architectural Components

The codebase is strictly divided into two primary, decoupled components: `/alerting` and `/webui`.

### 1. Alerting Engine (`/alerting`)
This is the backend daemon. It runs continuously (or via cron) and handles all alerting business logic.
- **Execution:** `monitor.py` acts as the entrypoint. It queries the API, evaluates threshold rules (disk space, GC age), and executes machine-learning logic to deduce snapshot patterns.
- **State Management:** Operates statelessly inside memory but persists findings and user settings to local JSON files (`config.json`, `state.json`, `group_rules.json`, `notification_log.json`) in `ALERTING_DATA_DIR` or the `alerting/` directory.
- **Schedule Inference:** `schedule.py` parses temporal gaps between historical backups to automatically infer if a datastore backs up "daily at 02:00" or "every 4 hours". If an expected window is missed, it flags an anomaly.
- **Notifications:** `notification.py` handles dispatching the alerts via the `ntfy` protocol, applying cooldown logic and quiet-hours silences.

### 2. Web UI (`/webui`)
This is the frontend dashboard. It provides a visual overlay over the backend state.
- **Execution:** `app.py` serves a Flask REST API and hosts the frontend static files. In normal mode it is served via `waitress`; with `FLASK_DEBUG=1` it uses Flask's development server.
- **Data Rendering:** It relies on `alerting_ui.py` to bridge the gap between backend objects and the frontend. It translates `Alert` objects into CSS health classes and compiles flat schedules.
- **Proxying:** The Flask app proxies data requests directly to the remote API (for Backup Browsing, etc.) so the frontend never directly needs the API Bearer Token.
- **Configuration Editing:** The UI acts as a graphical editor for the backend JSON files, using `validators.py` and `normalizers.py` to ensure user inputs don't corrupt the daemon. 

### 3. Multi-Source Adapter Pattern (`/alerting/sources/`)
Since version 1.2 the monitor is not tied to a single HTTP endpoint. Every data origin is wrapped by a `SourceAdapter` ([alerting/sources/base.py](../alerting/sources/base.py)) that exposes a small, uniform contract:

- `source_id` — stable slug used in alert keys and dashboard routing
- `check_health()` → `HealthResult(status, message, …)`
- `fetch_datastores()` → list of normalised datastore dicts
- `fetch_backup_inventory(datastore_id, snapshot_cap=…)` → namespaces/groups/snapshots
- `fetch_rescale_log(datastore_id)` → cloud-only, PBS returns `[]`

Two implementations ship today:
- **`RemoteBackupsAdapter`** ([alerting/sources/remote_backups.py](../alerting/sources/remote_backups.py)) — the original `api.remote-backups.com` client.
- **`PbsAdapter`** ([alerting/sources/pbs.py](../alerting/sources/pbs.py)) — talks to a local Proxmox Backup Server via its native API; see [docs/pbs_source.md](pbs_source.md).

Configuration is a list under `config.sources[]`; `validate_source_entries()` normalises it (an empty list synthesises an implicit `remote_backups` entry from the legacy top-level `api_base`), then `build_source(entry)` returns the right adapter.

```
   config.sources[]
          │
          ▼
  validate_source_entries()  ──►  build_source(entry)  ──►  SourceAdapter
          │                                                      │
          └── fail-closed error (ValueError)                      ▼
                                                        monitor.run_check()
                                                        webui.app routes
```

Every datastore dict carries `source_type` (`"remote_backups"` or `"pbs"`) and `source_id`. PBS datastore IDs use the `pbs:<source_id>:<store>` prefix so routing (e.g. the WebUI backup browser) can resolve the owning adapter without a state lookup.

**Error isolation.** The monitor loop and the WebUI routes catch `requests.RequestException`, `RuntimeError`, and `SourceConnectionError` per source — a single misbehaving adapter degrades its own entries only; the remaining sources keep working.

## Data Flow & File Persistence
The application uses the local file system (via atomic tmp-file writes) as the shared database between the daemon and the UI:
1. **`config.json`**: Holds thresholds, ntfy settings, heartbeat URLs (success + fail), daemon interval, quiet hours, schedule-learning settings, ignored groups, and notification priorities (global warning/critical tiers plus optional per-alert-type overrides). Editable via UI, consumed by daemon.
2. **`state.json`**: Holds the cached API inventory, snapshot history rings, and generated ML schedules. Written by the daemon, read by the UI.
3. **`group_rules.json`**: Holds manually overriden ("locked") schedules. Editable via UI, consumed by daemon.
4. **`notification_log.json`**: A rolling log of the last 500 dispatched push notifications.

## Design Philosophy & Safety
- **Stateless Modules:** Files like `normalization.py` and `normalizers.py` are strictly stateless to maximize testability.
- **Zero-Secret Logging:** Strict sentinel values (`_TOKEN_SENTINEL`) ensure ntfy tokens are never rendered back to the browser. API keys are read from environment variables rather than the alerting config.
- **Dependency Minimization:** The project keeps dependencies small and explicit: `requests`/`python-dotenv` for alerting, plus `flask`, `flask-limiter`, and `waitress` for the Web UI.
