# PBS Monitor

> **[English Version](README.md)**

Monitoring-Tools für [remote-backups.com](https://remote-backups.com) Datastores (Proxmox Backup Server as a Service).

Zwei unabhängige Werkzeuge:

1. **Web UI** — Dark-Theme Dashboard zur ad-hoc Statusprüfung aller Datastores
2. **Alerting** — Automatisiertes Monitoring mit Push-Benachrichtigungen via [ntfy](https://ntfy.sh)

Die Web UI kann dieselben Alarmzustände zusätzlich visuell darstellen, das
eigentliche Alerting-Script bleibt aber vollständig eigenständig und ist für
den unabhängigen Betrieb per Cron oder ähnlichem Scheduler gedacht.

Beide nutzen die [Monitoring API](https://api.remote-backups.com/reference#tag/monitoring-datastores) von remote-backups.com.

![Python](https://img.shields.io/badge/python-3.9+-green)

---

## Voraussetzungen

- Python 3.9+
- Ein [remote-backups.com](https://remote-backups.com) Account mit mindestens einem Datastore
- Ein Monitoring API Token (generierbar unter [Dashboard → Settings → Security](https://dashboard.remote-backups.com/settings/security))

## Setup

```bash
git clone https://github.com/maschkef/PBS_monitor
cd PBS_monitor

# API Key konfigurieren
cp .env.example .env
# .env editieren und API_KEY eintragen
```

---

## Tool 1: Web UI

Ein grafisches Dashboard um den Status aller Datastores auf einen Blick zu prüfen.

### Features

- **Storage-Gauge** mit Farbcodierung (grün < 80% < gelb < 90% < rot)
- **GC- & Verification-Status** als Badges mit Zeitangaben (letzte Ausführung, nächster Lauf)
- **Retention-Policy** — Übersicht der Prune-Konfiguration (keep last/hourly/daily/weekly/monthly/yearly)
- **Autoscaling-Konfiguration** — Schwellwerte und Modus
- **Immutable Backup & Replication Status**
- **Backup Browser** — PBS-Namespaces, spezifische Backup-Gruppen, Snapshots und andere Protokolle (rsync, sftp, zfs-recv) direkt in der UI durchsuchen; jeder Snapshot zeigt seinen Verifikationsstatus (verified / verify failed / unverified)
- **Editierbare Gruppen-Schedules** — gelernte Zeitpläne können in der Web UI geprüft, angepasst und gesperrt werden
- **Ignorierte Gruppen** — Alerts für spezifische Backup-Gruppen direkt über das Web-Interface stummschalten
- **Rescale-History** — Timeline der letzten 90 Tage (Autoscaling-Events, manuelle Resizes)
- **Visuelles Alerting** — aktuelle Alarmzustände und gelernte Backup-Fenster direkt im Dashboard
- **Platform-Stats** — Gesamtspeicher, Backup-Count und Traffic der Plattform
- **Zweistufige Aktualisierung** — der **⟳ Refresh**-Button führt eine vollständige Aktualisierung durch (alle Daten inkl. Rescale-Log, Backup-Inventar und Platform-Stats); der **Auto-Refresh**-Timer lädt dagegen nur häufig wechselnde Daten nach (Speicher-Metriken, GC-/Verification-Zeitangaben, Replikations-Sync, Alerting-Zustand). Das reduziert die API-Aufrufe beim automatischen Refresh von ~22+ auf ~4 (bei einem typischen Account mit drei Datastores). Über jeden Button und das Intervall-Dropdown zeigt ein Tooltip, was dabei geladen bzw. ausgelassen wird.
- **Auto-Refresh** Toggle mit konfigurierbaren Intervallen von 5 bis 30 Minuten, Standard: 10 Minuten
- **Gesundheitsbewertung** pro Datastore (healthy / warning / critical)

### Starten

```bash
cd webui
pip install -r requirements.txt
python app.py
```

Dashboard öffnen: [http://127.0.0.1:5111](http://127.0.0.1:5111)

> **Produktivbetrieb:** Die App wird standardmäßig von [Waitress](https://docs.pylonsproject.org/projects/waitress/) ausgeliefert (in `requirements.txt` enthalten), wodurch die Flask-Development-Server-Warnung entfällt. Mit `FLASK_DEBUG=1` in `.env` kann bei Bedarf wieder auf den Flask Dev-Server mit Auto-Reload umgeschaltet werden.

### Screenshot

Das Dashboard zeigt pro Datastore eine Karte mit vier Sektionen:

| Sektion | Inhalt |
|---------|--------|
| **Storage** | Auslastung in %, verwendet/frei in GB, Backup-Anzahl |
| **Jobs** | GC-Status und Verification-Status mit Zeitangaben |
| **Retention** | Prune-Schedule und Keep-Werte als Übersicht |
| **Features** | Autoscaling, Immutable Backups, Replication |

---

## Tool 2: Alerting

Ein Monitoring-Script das regelmäßig den Status prüft und bei Problemen Push-Benachrichtigungen via [ntfy](https://ntfy.sh) sendet.

Dieses Script ist absichtlich unabhängig von der Web UI, damit es eigenständig
auf einem Server per Cron laufen kann.

### Features

- **Storage-Überwachung** — Warnung bei 80%, kritisch bei 90%
- **GC-Überwachung** — Alert bei Fehler oder wenn GC überfällig ist (> 36h)
- **Verification-Überwachung** — Alert bei Fehler oder überfällig (> 14 Tage)
- **Backup-Inventarisierung** — sammelt PBS-Snapshots namespace- und gruppengenau als Grundlage für lernendes Alerting
- **Totalausfall-Erkennung** — Sofort-Alarm wenn Backup-Browser und Aggregat-Metrik gemeinsam auf 0 fallen
- **Gelernte Backup-Fenster** — leitet konservative Wochentag-/Zeit-Slots pro Backup-Gruppe aus beobachteten Snapshots ab
- **Missed-Backup-Alerts** — warnt bei verpassten gelernten Backup-Fenstern und behandelt manuelle Off-Schedule-Läufe als Ausreißer
- **Gesperrte Gruppenregeln** — manuelle Zeitpläne können das Lernen für einzelne Backup-Gruppen übersteuern
- **Ignorierte Gruppen** — Backup-Gruppen können über die UI oder Konfiguration komplett vom Monitoring ausgeschlossen werden
- **Replication-Lag-Alerts** — warnt wenn eine konfigurierte Replikation deutlich hinterherhängt
- **Host-Offline-Erkennung** — Alert wenn der Server nicht erreichbar ist
- **Immutable Backup Warnung** — Alert bei pending Disable-Request
- **API-Health-Check** — Prüft die Plattform-Erreichbarkeit
- **Quiet Hours** — Niedrig-priore Alerts nachts unterdrücken
- **Alert-Cooldown** — Verhindert Spam bei anhaltenden Problemen
- **Persistenter State** — versionierte Snapshot-Historie pro Backup-Gruppe

### Setup

```bash
cd alerting
pip install -r requirements.txt

# Konfiguration erstellen und anpassen
cp config.json.example config.json
# config.json editieren: ntfy_topic setzen (Pflicht), optional ntfy_token
```

### Nutzung

```bash
# Einmaliger Check
python monitor.py

# Daemon-Modus (alle 5 Minuten)
python monitor.py --daemon 300
```

### Cron-Job (empfohlen)

```bash
# Alle 5 Minuten prüfen
*/5 * * * * cd /pfad/zu/PBS_monitor/alerting && /usr/bin/python3 monitor.py >> /var/log/pbs-monitor.log 2>&1
```

### Konfiguration

Die Datei `alerting/config.json` wird beim ersten Start automatisch aus `alerting/config.json.example` kopiert, falls sie noch nicht existiert. Du kannst sie jedoch auch vorab manuell erstellen:

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

Manuelle und gesperrte Gruppen-Schedules werden separat in `alerting/group_rules.json` gespeichert.
Unterstützte manuelle Schedule-Typen sind `daily`, `weekly` und `interval`.

| Parameter | Beschreibung |
|-----------|-------------|
| `ntfy_topic` | ntfy Topic-Name — **muss angepasst werden** |
| `ntfy_token` | Optional. Bearer Token für private ntfy-Instanzen |
| `ntfy_url` | ntfy Server URL (default: `https://ntfy.sh`) |
| `ignored_groups` | Liste von Backup-Gruppen (Datastore, Namespace, Typ, ID), für die keine Alerts generiert werden sollen |
| `storage_warn_percent` | Speicher-Warnung ab diesem Prozentsatz |
| `storage_crit_percent` | Speicher-Kritisch ab diesem Prozentsatz |
| `gc_max_age_hours` | GC gilt als überfällig nach X Stunden |
| `verification_max_age_days` | Verification gilt als überfällig nach X Tagen |
| `quiet_hours.enabled` | Quiet Hours aktivieren (true/false) |
| `quiet_hours.min_priority` | Nur Alerts ab dieser Priorität während Quiet Hours senden |
| `schedule_learning.enabled` | Lernende Backup-Fenster-Erkennung aktivieren |
| `schedule_learning.timezone` | Zeitzone für die Schedule-Erkennung. `local` oder eine IANA-Zeitzone wie `Europe/Berlin` |
| `schedule_learning.history_window_days` | Wie viele Tage Snapshot-Historie für das Lernen berücksichtigt werden |
| `schedule_learning.min_occurrences` | Benötigte passende Beobachtungen pro Wochentag/Zeit-Slot, bevor er aktiv wird |
| `schedule_learning.time_tolerance_minutes` | Erlaubte zeitliche Abweichung in Minuten für das Lernen und Matchen. Standard: `30` |
| `schedule_learning.due_grace_minutes` | Wie lange ein gelerntes Backup-Fenster verspätet sein darf, bevor ein Alert erzeugt wird. Standard: `30` |
| `schedule_learning.stale_after_days` | Zusätzliche Tage über die normale wöchentliche Slot-Kadenz hinaus, bevor ein gelernter Slot als veraltet gilt |
| `alert_cooldown_minutes` | Mindestzeit zwischen wiederholten Alerts gleichen Typs |

### Alert-Prioritäten (ntfy)

| Prio | Verwendung |
|------|-----------|
| 5 (urgent) | Storage ≥ 90%, Verification failed, alle Backups weg, API nicht erreichbar |
| 4 (high) | GC failed, Host offline, verpasstes Backup-Fenster oder Intervall, veraltete Replication, Immutable Disable pending |
| 3 (default) | Storage ≥ 80%, GC/Verification überfällig oder nie gelaufen |

---

## API-Limitierungen

Die Monitoring API ist read-only. Sie liefert inzwischen live PBS-Namespaces, Backup-Gruppen und Snapshots, aber Folgendes ist darüber weiterhin **nicht** verfügbar:

- Ob ein Snapshot aus einem automatischen oder manuellen Lauf stammt
- Per-Snapshot Verification-Status
- Konfigurierte Backup-Schedules oder -Frequenzen pro Gruppe
- I/O-Graphen oder langfristige Zeitreihen-Daten

Das Alerting persistiert deshalb jetzt die Backup-Browser-Daten pro Namespace und Gruppe und lernt daraus konservative Wochentag-/Zeit-Slots oder kurze Intervalle. Aktuell kann das Backup-Alerting erkennen:
- ✅ Ob alle sichtbaren PBS-Backups verschwunden sind
- ✅ Ob ein gelerntes wiederkehrendes Backup-Fenster für eine bestimmte Backup-Gruppe verpasst wurde
- ✅ Häufige wiederkehrende Backups wie alle 2 Stunden per Intervall-Erkennung
- ✅ Täglich wiederkehrende Backups als eigener editierbarer Schedule-Typ
- ✅ Off-Schedule-Snapshots am selben Tag als Kontext, ohne sie als Beweis für einen erfolgreichen geplanten Lauf zu werten
- ❌ Komplexere Rhythmen wie monatliche, zweiwöchentliche oder wirklich unregelmäßige Schedules

### Genutzte Endpoints

| Endpoint | Auth | Beschreibung |
|----------|------|-------------|
| `GET /monitoring/v1/datastores` | Bearer | Alle Datastores mit Live-Metriken |
| `GET /monitoring/v1/datastores/{id}` | Bearer | Detail inkl. Prune, Autoscaling, Replication |
| `GET /monitoring/v1/datastores/{id}/backups` | Bearer | Namespace-aware PBS-Backup-Inventar |
| `GET /monitoring/v1/datastores/{id}/rescale-log` | Bearer | Resize-Historie |
| `GET /health` | — | Plattform-Gesundheit |
| `GET /public/total-storage` | — | Gesamtspeicher Plattform |
| `GET /public/backups-30-days` | — | Backup-Count Plattform (30 Tage) |
| `GET /public/traffic-30-days` | — | Traffic Plattform (30 Tage) |

---

## Projektstruktur

```
PBS_monitor/
├── .env.example                    # Vorlage für API Key
├── .gitignore
├── README.md
├── webui/                          # Tool 1: Web Dashboard
│   ├── app.py                      # Flask Server
│   ├── requirements.txt
│   └── templates/
│       └── index.html              # Single-Page Dashboard
└── alerting/                       # Tool 2: Monitoring + Alerting
    ├── monitor.py                  # Monitoring Script
    ├── requirements.txt
    ├── config.json.example         # Vorlage für Alerting-Konfiguration
    ├── config.json                 # Lokale Konfig (gitignored)
    ├── group_rules.json            # Lokale Gruppenregeln (gitignored, auto-generiert)
    └── state.json                  # Runtime-State (gitignored, auto-generiert)
```

## Disclaimer

Dieses Projekt ist nicht mit remote-backups.com verbunden, wird nicht von ihnen gewartet oder unterstützt.

## Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert – siehe die [LICENSE](LICENSE) Datei für Details.
