# Technical Documentation: webui/app.py

## Overview
This file is the main entry point for the Flask-based Web User Interface of the PBS Monitor. It exposes a set of REST APIs consumed by the frontend JavaScript (like `dashboard.js`), handles authentication, proxies requests to the upstream `remote-backups.com` API, and integrates directly with the alerting backend configuration and state to provide the UI.

## Application Setup & Security

### Security Configuration
- **Rate Limiting:** `flask_limiter` is configured globally. The default limit is 200 requests per minute, with specific stricter limits on login (10/min) and testing endpoints (10/min) to prevent brute force while remaining usable for a single admin.
- **Reverse Proxy Support:** Handles `X-Forwarded-For` and `X-Real-IP` if `WEBUI_PROXY_COUNT > 0` using Werkzeug's `ProxyFix`.
- **Cookies & Sessions:** Uses a random 32-byte hex `secret_key` for session signing. Cookies are flagged `HttpOnly`, `SameSite=Lax`, and `Secure` (if `WEBUI_SECURE_COOKIES` is 1).
- **Security Headers:** The `@app.after_request` decorator injects strict headers like `X-Frame-Options: DENY`, and `Permissions-Policy`. The `Content-Security-Policy` header is restrictive by default but can be overridden via the `WEBUI_CSP` environment variable to support embedding or reverse-proxy injections.
- **Audit Logging:** Emits simple text-based log lines to the Flask standard logger for security-relevant actions (logins, config changes, testing) to maintain an audit trail without enterprise SIEM overhead.

### Authentication Middlewares
- **`@require_auth`**: Decorates routes to demand a valid session. Redirects to `/login` for HTML routes, or returns `401 Unauthorized` for `/api/` JSON routes. Can be bypassed entirely if no `WEBUI_PASSWORD` is set.
- **`@require_csrf`**: Decorates state-changing `POST`/`DELETE` routes. Validates the `X-CSRF-Token` header against the token stored in the user's session. Returns `403` on failure.

## Route Definitions

### HTML Views
- **`/login` (GET/POST)**: Handles password submission. Compares `WEBUI_PASSWORD` and sets the `authenticated` session flag on success. Also validates a hidden form nonce against login-CSRF.
- **`/logout` (POST)**: Clears the session dictionary.
- **`/` (GET)**: Serves the `index.html` dashboard SPA.

### Read-Only API Endpoints
These endpoints fetch data to populate the frontend tables.

- **`/api/datastores`**: The heavy "full load" endpoint. Proxies to `/monitoring/v1/datastores`, fetching details (rescale logs, metric history, GC, replication). Calls `alerting_ui.build_visual_alerting` to map the backend rules into visual `issues` and `health` labels.
- **`/api/datastores/metrics`**: A lightweight, fast-refresh variant of `/api/datastores`. Skips deep backup inventory fetches and rescale logs, evaluating alerts strictly against the cached `state.json`.
- **`/api/datastores/<id>/backups`**: Proxies requests for the Backup Browser view. Groups results using `normalizers.normalize_namespace`. Fetches legacy protocol metrics (rsync, sftp, zfs-recv).
- **`/api/health`** & **`/api/platform-stats`**: Proxies to public unauthenticated `remote-backups.com` APIs.
- **`/api/alerting/config` (GET)**: Returns the current config.json with secrets stripped (using `validators._redact_config`).
- **`/api/alerting/notification-log` (GET)**: Reads and returns the notification history log.
- **`/api/webui/info`**: Returns diagnostic data (Docker status, read-only mode).

### State-Changing API Endpoints (POST / DELETE)
These routes modify configuration and state. They enforce CSRF tokens and the `read_only_guard()` (which aborts with `403` if `WEBUI_READ_ONLY` is enabled).

- **`/api/alerting/config` (POST)**: 
  Saves user modifications to the backend configuration.
  **Logic:** Validates the incoming payload against `validators._validate_config_payload`. Uses `_write_json_atomic` to safely rewrite `config.json`.
- **`/api/alerting/group-rule` (POST)**:
  Configures or overrides a specific scheduled backup rule (interval, daily, weekly, or none) for a datastore group. Persists to `group_rules.json`.
- **`/api/alerting/ignore-group` (POST) / `/api/alerting/unignore-group` (POST)**:
  Adds or removes a specific backup group from the `ignored_groups` array in `config.json`, silencing alerts for it.

### Testing Endpoints
- **`/api/alerting/test/dry-run`**: Triggers a simulated run of `monitor.py` logic. Calculates active alerts but stops before invoking `send_ntfy`.
- **`/api/alerting/test/live`**: Actually calls `alert_monitor.run_check(config, state)` and captures its `stdout` output string.
- **`/api/alerting/test/notify`**: Uses `requests.post` to dispatch a standalone "Test Notification" string to the configured ntfy topic.

## Production Execution
If `FLASK_DEBUG` is false, it imports and binds the WSGI application to the `waitress` production server on `WEBUI_HOST:WEBUI_PORT` instead of using the Flask dev server.
