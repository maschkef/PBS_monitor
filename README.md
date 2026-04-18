# PBS Monitor

> **[Deutsche Version](README_DE.md)**

Monitoring tools for [remote-backups.com](https://remote-backups.com) datastores. Primarily built and tested against Proxmox Backup Server (PBS) datastores. The Web UI also displays rsync, SFTP, and ZFS-recv backup data when available; the alerting script is PBS-only.

Two independent tools:

1. **Web UI** — Dark-theme dashboard for ad-hoc status checks across all datastores
2. **Alerting** — Automated monitoring with push notifications via [ntfy](https://ntfy.sh)

The Web UI can surface the same alert conditions visually, but the alerting
script remains fully standalone and is intended to run independently via cron
or a similar scheduler.

Both use the [Monitoring API](https://api.remote-backups.com/reference#tag/monitoring-datastores) from remote-backups.com.

![Python](https://img.shields.io/badge/python-3.9+-green)

> [!NOTE]
> This project is not affiliated with, maintained, or endorsed by remote-backups.com.

---

## Prerequisites

- Python 3.9+
- A [remote-backups.com](https://remote-backups.com) account with at least one datastore
- A Monitoring API token (generate at [Dashboard → Settings → Security](https://dashboard.remote-backups.com/settings/security))

## Setup

```bash
git clone https://github.com/maschkef/PBS_monitor
cd PBS_monitor

# Configure API key
cp .env.example .env
# Edit .env and set your API_KEY
```

---

## Tool 1: Web UI

A graphical dashboard to check the status of all datastores at a glance.

### Features

- **Storage gauge** with color coding (green < 80% < yellow < 90% < red)
- **GC & verification status** as badges with timestamps (last run, next scheduled)
- **Retention policy** — overview of prune configuration (keep last/hourly/daily/weekly/monthly/yearly)
- **Autoscaling configuration** — thresholds and mode
- **Immutable backup & replication status**
- **Backup browser** — explore PBS namespaces, backup groups, individual snapshots, and other protocols (rsync, sftp, zfs-recv) directly in the UI; each snapshot shows its verification status (verified / verify failed / unverified)
- **Editable group schedules** — learned schedules can be reviewed, edited, and locked from the Web UI
- **Ignored groups** — mute alerts for specific backup groups directly via the web interface
- **Rescale history** — timeline of the last 90 days (autoscaling events, manual resizes)
- **Visual alerting** — current alert conditions and learned backup windows directly in the dashboard
- **Platform stats** — total storage, backup count and traffic across the platform
- **Two-tier refresh** — the **⟳ Refresh** button performs a full reload (all data including rescale-log, backup inventory, and platform stats); the **Auto-Refresh** timer runs a lightweight update that only fetches frequently-changing data (storage metrics, GC/verification timestamps, replication sync times, alerting state). This reduces API calls during auto-refresh from ~22+ to ~4 for a typical three-datastore account. Hover over each button or control for a tooltip describing what is and isn't refreshed.
- **Auto-Refresh** toggle with configurable intervals from 5 to 30 minutes, defaulting to 10 minutes
- **Health assessment** per datastore (healthy / warning / critical)

### Start

```bash
cd webui
pip install -r requirements.txt
python app.py
```

Open the dashboard: [http://127.0.0.1:5111](http://127.0.0.1:5111)

> **Production note:** By default the app is served by [Waitress](https://docs.pylonsproject.org/projects/waitress/) (included in `requirements.txt`), which avoids the Flask development-server warning. Set `FLASK_DEBUG=1` in `.env` to switch back to the Flask dev server with auto-reload.

### Dashboard Sections

Each datastore is displayed as a card with four sections:

| Section | Content |
|---------|---------|
| **Storage** | Usage in %, used/free in GB, backup count |
| **Jobs** | GC status and verification status with timestamps |
| **Retention** | Prune schedule and keep values overview |
| **Features** | Autoscaling, immutable backups, replication |

---

## Tool 2: Alerting

A monitoring script that periodically checks datastore health and sends push notifications via [ntfy](https://ntfy.sh) when problems are detected.

This script is intentionally independent of the Web UI so it can run on its own
on a server via cron.

### Features

- **Storage monitoring** — warning at 80%, critical at 90%
- **GC monitoring** — alert on failure or when overdue (> 36h)
- **Verification monitoring** — alert on failure or overdue (> 14 days)
- **Backup inventory tracking** — collects namespace- and group-level PBS snapshot history for later learned alerting
- **Total loss detection** — immediate alarm when both backup browser and aggregate metrics drop to zero
- **Learned backup windows** — derives conservative weekday/time slots per backup group from observed snapshots
- **Missed backup alerts** — warns when a learned backup window is missed while off-schedule manual runs are treated as outliers
- **Locked group rules** — manual schedules can override learning for specific backup groups
- **Ignored groups** — specific backup groups can be completely excluded from monitoring via UI or configuration files
- **Replication lag alerts** — warns when configured replication falls noticeably behind
- **Host offline detection** — alert when the server is unreachable
- **Immutable backup warning** — alert on pending disable request
- **API health check** — verifies platform availability
- **Quiet hours** — suppress low-priority alerts at night
- **Alert cooldown** — prevents spam for persistent issues
- **Persistent state** — versioned per-group snapshot history retained across runs

### Setup

```bash
cd alerting
pip install -r requirements.txt

# Create and edit configuration
cp config.json.example config.json
# Edit config.json: set ntfy_topic (required), optionally ntfy_token
```

### Usage

```bash
# Single check
python monitor.py

# Daemon mode (every 5 minutes)
python monitor.py --daemon 300
```

### Cron Job (recommended)

```bash
# Check every 5 minutes
*/5 * * * * cd /path/to/PBS_monitor/alerting && /usr/bin/python3 monitor.py >> /var/log/pbs-monitor.log 2>&1
```

### Configuration

The file `alerting/config.json` will be automatically copied from `alerting/config.json.example` on the first run if you don't create it manually:

```json
{
  "_comment_api": "Base URL of the Monitoring API",
  "api_base": "https://api.remote-backups.com",
  
  "_comment_ntfy": "Notification settings. ntfy_topic is required. ntfy_token is optional for private ntfy instances.",
  "ntfy_url": "https://ntfy.sh",
  "ntfy_topic": "your-topic-here",
  "ntfy_token": "",
  
  "_comment_ignored": "List of objects describing backup groups to ignore.",
  "ignored_groups": [],
  
  "_comment_thresholds": "Warning and critical thresholds for datastore events.",
  "thresholds": {
    "storage_warn_percent": 80,
    "storage_crit_percent": 90,
    "gc_max_age_hours": 36,
    "verification_max_age_days": 14
  },
  
  "_comment_quiet_hours": "Suppresses lower-priority alerts during specified hours.",
  "quiet_hours": {
    "enabled": false,
    "start": "22:00",
    "end": "07:00",
    "min_priority": 4
  },
  
  "_comment_learning": "Toggles dynamic learning for missed backup window detection.",
  "schedule_learning": {
    "enabled": true,
    "timezone": "local",
    "history_window_days": 60,
    "min_occurrences": 2,
    "time_tolerance_minutes": 30,
    "due_grace_minutes": 30,
    "stale_after_days": 8
  },
  
  "_comment_cooldown": "Minimum minutes to wait before repeating an alert of the same type.",
  "alert_cooldown_minutes": 60
}
```

Per-group manual and locked schedules are stored separately in `alerting/group_rules.json`.
Supported manual schedule types are `daily`, `weekly`, and `interval`.

| Parameter | Description |
|-----------|-------------|
| `ntfy_topic` | ntfy topic name — **must be configured** |
| `ntfy_token` | Optional. Bearer token for private ntfy instances |
| `ntfy_url` | ntfy server URL (default: `https://ntfy.sh`) |
| `ignored_groups` | List of backup groups (datastore, namespace, type, id) to exclude from alert generation |
| `storage_warn_percent` | Storage warning threshold in percent |
| `storage_crit_percent` | Storage critical threshold in percent |
| `gc_max_age_hours` | GC considered overdue after X hours |
| `verification_max_age_days` | Verification considered overdue after X days |
| `quiet_hours.enabled` | Enable quiet hours (true/false) |
| `quiet_hours.min_priority` | Only send alerts at or above this priority during quiet hours |
| `schedule_learning.enabled` | Enable learned backup-window detection |
| `schedule_learning.timezone` | Timezone used for schedule learning. Use `local` or an IANA timezone such as `Europe/Berlin` |
| `schedule_learning.history_window_days` | How many days of observed snapshot history are considered for learning |
| `schedule_learning.min_occurrences` | Required matching observations per weekday/time slot before it becomes active |
| `schedule_learning.time_tolerance_minutes` | Allowed schedule deviation in minutes for learning and slot matching. Default: `30` |
| `schedule_learning.due_grace_minutes` | How long a learned backup window may be late before a missed-backup alert is emitted. Default: `30` |
| `schedule_learning.stale_after_days` | Extra days beyond the normal weekly slot cadence before a learned slot is treated as stale |
| `alert_cooldown_minutes` | Minimum time between repeated alerts of the same type |

### Alert Priorities (ntfy)

| Priority | Usage |
|----------|-------|
| 5 (urgent) | Storage ≥ 90%, verification failed, all backups gone, API unreachable |
| 4 (high) | GC failed, host offline, missed backup window or interval, stale replication, immutable disable pending |
| 3 (default) | Storage ≥ 80%, GC/verification overdue or never ran |

---

## API Limitations

The Monitoring API is read-only. It now exposes live PBS namespaces, backup groups, and snapshots, but the following is still **not** available through this API:

- Whether a snapshot came from an automatic or manual run
- Per-snapshot verification status
- Configured backup schedules or frequencies per group
- I/O graphs or long-term time-series data

The alerting script now persists backup-browser inventory per namespace and group and learns conservative weekday/time slots or short intervals from that history. Current backup alerting can detect:
- ✅ Whether all visible PBS backups have disappeared
- ✅ Whether a learned recurring backup window was missed for a specific backup group
- ✅ Frequent recurring backups such as every 2 hours via interval detection
- ✅ Daily recurring backups as a dedicated editable schedule type
- ✅ Off-schedule same-day snapshots as context without treating them as proof that the learned window ran
- ❌ More complex cadences such as monthly, biweekly, or truly irregular schedules

### Endpoints Used

| Endpoint | Auth | Description |
|----------|------|-------------|
| `GET /monitoring/v1/datastores` | Bearer | All datastores with live metrics |
| `GET /monitoring/v1/datastores/{id}` | Bearer | Details incl. prune, autoscaling, replication |
| `GET /monitoring/v1/datastores/{id}/backups` | Bearer | Namespace-aware PBS backup inventory |
| `GET /monitoring/v1/datastores/{id}/backups/rsync` | Bearer | rsync backup data (Web UI) |
| `GET /monitoring/v1/datastores/{id}/backups/sftp` | Bearer | SFTP backup data (Web UI) |
| `GET /monitoring/v1/datastores/{id}/backups/zfs-recv` | Bearer | ZFS-recv backup data (Web UI) |
| `GET /monitoring/v1/datastores/{id}/rescale-log` | Bearer | Resize history |
| `GET /health` | — | Platform health |
| `GET /public/total-storage` | — | Total platform storage |
| `GET /public/backups-30-days` | — | Platform backup count (30 days) |
| `GET /public/traffic-30-days` | — | Platform traffic (30 days) |

---

## Project Structure

```
PBS_monitor/
├── .env.example                    # API key template
├── .gitignore
├── README.md                       # English documentation
├── README_DE.md                    # German documentation
├── webui/                          # Tool 1: Web Dashboard
│   ├── app.py                      # Flask server
│   ├── requirements.txt
│   └── templates/
│       └── index.html              # Single-page dashboard
└── alerting/                       # Tool 2: Monitoring + Alerting
    ├── monitor.py                  # Monitoring script
    ├── requirements.txt
    ├── config.json.example         # Alerting configuration template
    ├── config.json                 # Local config (gitignored)
    ├── group_rules.json            # Local per-group rules (gitignored, auto-generated)
    └── state.json                  # Runtime state (gitignored, auto-generated)
```

---

## Contact

If you have questions, suggestions, or encounter issues with this project, feel free to reach out:

📧 **Email:** [maschkef-git@pm.me](mailto:maschkef-git@pm.me)

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
