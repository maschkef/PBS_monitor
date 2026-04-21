# PBS Monitor

> **[English Version](README.md)**

Monitoring-Tools für [remote-backups.com](https://remote-backups.com) Datastores. Primär entwickelt und getestet für Proxmox Backup Server (PBS) Datastores. Die Web UI zeigt zusätzlich rsync-, SFTP- und ZFS-recv-Backupdaten an, sofern vorhanden; das Alerting-Script unterstützt ausschließlich PBS.

Zwei unabhängige Werkzeuge:

1. **Web UI** — Dark-Theme Dashboard zur ad-hoc Statusprüfung aller Datastores
2. **Alerting** — Automatisiertes Monitoring mit Push-Benachrichtigungen via [ntfy](https://ntfy.sh)

Die Web UI kann dieselben Alarmzustände zusätzlich visuell darstellen, das
eigentliche Alerting-Script bleibt aber vollständig eigenständig und ist für
den unabhängigen Betrieb per Cron oder ähnlichem Scheduler gedacht.

**Integration**: Wenn beide Tools aktiv sind, kann die Web UI zur Konfiguration
des Alerting-Systems verwendet werden (Zeitpläne, Schwellwerte, ignorierte Gruppen,
ntfy-Einstellungen) über eine Web-Oberfläche anstatt manueller Bearbeitung der
Konfigurationsdateien.

Beide nutzen die [Monitoring API](https://api.remote-backups.com/reference#tag/monitoring-datastores) von remote-backups.com.

![Python](https://img.shields.io/badge/python-3.9+-green)
![Docker](https://img.shields.io/badge/docker-verfügbar-blue)

> [!TIP]  
> **🐳 Docker-Bereitstellung:** Docker-Unterstützung ist verfügbar und wurde bei mir erfolgreich getestet.
> 
> **Schnellstart:**
> ```bash
> # Option 1: Ein-Befehl-Bereitstellung
> curl -sL https://raw.githubusercontent.com/maschkef/PBS_monitor/main/docker/quick-deploy.sh | bash
> 
> # Option 2: Traditioneller docker-compose Workflow
> wget https://github.com/maschkef/PBS_monitor/releases/latest/download/docker-compose.yml
> wget https://raw.githubusercontent.com/maschkef/PBS_monitor/main/.env.example -O .env
> # .env editieren und API_KEY setzen, dann:
> docker-compose up -d
> ```
> 
> Siehe Release-Assets für Dokumentation: [Neueste Version](https://github.com/maschkef/PBS_monitor/releases/latest)

> [!NOTE]
> Dieses Projekt steht in keiner Verbindung zu remote-backups.com und wird weder von ihnen betrieben noch offiziell unterstützt.

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
- **Alerting-Konfiguration** — wenn das Alerting-System aktiv ist, bietet die Web UI eine vollständige Oberfläche zur Konfiguration aller Alerting-Einstellungen: Zeitpläne, Schwellwerte, ignorierte Gruppen, ntfy-Einstellungen, Ruhezeiten, Benachrichtigungs-Prioritäten und mehr
- **Editierbare Gruppen-Schedules** — gelernte Zeitpläne können in der Web UI geprüft, angepasst und gesperrt werden; Intervall-Schedules unterstützen eine optionale Ankerzeit (z. B. `06:00` → Backups erwartet um 06:00, 08:00, 10:00 …)
- **Nächstes Backup** — jede Backup-Gruppe im Alerting-Panel zeigt den berechneten nächsten erwarteten Backup-Zeitpunkt basierend auf dem aktiven Schedule
- **Ignorierte Gruppen** — Alerts für spezifische Backup-Gruppen direkt über das Web-Interface stummschalten; ignorierte Gruppen werden in einer ausklappbaren Liste angezeigt und können jederzeit reaktiviert werden (Unignore)
- **Rescale-History** — Timeline der letzten 90 Tage (Autoscaling-Events, manuelle Resizes)
- **Benachrichtigungs-Log** — persistente History aller gesendeten Alerts (inkl. Test-Benachrichtigungen); über den **📋 Log**-Button in der Kopfzeile einseh- und löschbar
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

### Authentifizierung

Die Web UI ist standardmäßig offen zugänglich. Um sie mit einem Passwort zu schützen, `WEBUI_PASSWORD` in `.env` setzen:

```ini
WEBUI_PASSWORD=dein-sicheres-passwort
# Optional, aber empfohlen für stabile Sessions über Neustarts hinweg:
# Key generieren mit: python3 -c "import secrets; print(secrets.token_hex(32))"
WEBUI_SECRET_KEY=generierten-key-hier-eintragen
```

Wenn `WEBUI_PASSWORD` gesetzt ist, werden nicht eingeloggte Besucher zur Login-Seite weitergeleitet. Leer lassen, wenn die Zugangskontrolle bereits auf Netzwerkebene erfolgt (z. B. Traefik + OAuth2-Proxy).

### Web UI Umgebungsvariablen

Die folgenden Variablen können in `.env` gesetzt werden:

| Variable | Standard | Beschreibung |
|----------|----------|--------------|
| `WEBUI_PASSWORD` | *(leer)* | Dashboard-Passwort. Leer lassen um Authentifizierung zu deaktivieren |
| `WEBUI_SECRET_KEY` | *(auto-generiert)* | Flask Session-Signing-Key. Wird beim Start automatisch generiert wenn nicht gesetzt (Sessions werden bei Neustart ungültig) |
| `WEBUI_PORT` | `5111` | Port auf dem der Webserver lauscht |
| `WEBUI_HOST` | `127.0.0.1` | Bind-Adresse. Auf `0.0.0.0` setzen um auf allen Interfaces erreichbar zu sein |
| `WEBUI_READ_ONLY` | `0` | Auf `1` setzen um alle Schreiboperationen zu deaktivieren (Konfig-Änderungen, Regeländerungen, Gruppen ignorieren, Live-Test) |
| `FLASK_DEBUG` | `0` | Auf `1` setzen um Flask Debug-Modus zu aktivieren. **Nicht** im Produktivbetrieb verwenden |
| `WEBUI_SECURE_COOKIES` | `0` | Auf `1` setzen um den `Secure`-Flag auf Session-Cookies zu setzen. Aktivieren wenn das Dashboard über HTTPS ausgeliefert wird (z. B. hinter einem TLS-terminierenden Reverse Proxy) |
| `WEBUI_HIDE_SERVER_PATHS` | `0` | Auf `1` setzen um serverseitige Dateisystempfade aus dem `/api/webui/info`-Endpoint auszublenden (Alerting-Datenverzeichnis, Python-Executable). Empfohlen wenn das Dashboard öffentlich erreichbar ist |
| `WEBUI_PROXY_COUNT` | `0` | Anzahl der vertrauenswürdigen Reverse-Proxy-Hops vor Flask. Wenn ungleich 0, liest Flask die echte Client-IP aus `X-Real-IP` (von Traefik standardmäßig gesetzt) oder aus `X-Forwarded-For`. Auf die Anzahl der selbst betriebenen Proxys setzen, z. B. `1` für einen einzelnen Traefik, `2` für nginx → Traefik → Flask |

> [!TIP]
> **Reverse-Proxy-Setup:** `WEBUI_PROXY_COUNT` auf die Anzahl der selbst betriebenen Proxy-Hops setzen. Wenn gesetzt, prüft Flask zuerst `X-Real-IP` (wird von Traefik direkt auf die Client-IP gesetzt) und fällt auf `X-Forwarded-For` zurück. So zeigen Rate-Limiting und Login-Audit-Logs immer die echte Client-IP. Zusätzlich `WEBUI_SECURE_COOKIES=1` aktivieren, wenn TLS vom Proxy (z. B. Traefik) terminiert wird.

### Dashboard-Sektionen

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
- **Snapshot-Verschwinden-Erkennung** — warnt wenn die Snapshot-Anzahl einer Gruppe unter die `keep_last`-Prune-Policy fällt und so eine unerwartete Löschung außerhalb des normalen Prunings anzeigt
- **Gelernte Backup-Fenster** — leitet konservative Wochentag-/Zeit-Slots pro Backup-Gruppe aus beobachteten Snapshots ab
- **Missed-Backup-Alerts** — warnt bei verpassten gelernten Backup-Fenstern und behandelt manuelle Off-Schedule-Läufe als Ausreißer
- **Gesperrte Gruppenregeln** — manuelle Zeitpläne können das Lernen für einzelne Backup-Gruppen übersteuern; Intervall-Schedules akzeptieren eine optionale Ankerzeit (HH:MM), sodass die Erwartung an einem festen Startzeitpunkt ausgerichtet ist statt am letzten beobachteten Backup
- **Ignorierte Gruppen** — Backup-Gruppen können über die UI oder Konfiguration komplett vom Monitoring ausgeschlossen und jederzeit über die Web UI wieder reaktiviert werden
- **Replication-Lag-Alerts** — warnt wenn eine konfigurierte Replikation deutlich hinterherhängt
- **Host-Offline-Erkennung** — Alert wenn der Server nicht erreichbar ist
- **Immutable Backup Warnung** — Alert bei pending Disable-Request
- **API-Health-Check** — Prüft die Plattform-Erreichbarkeit
- **Quiet Hours** — Niedrig-priore Alerts nachts unterdrücken
- **Konfigurierbare Benachrichtigungs-Prioritäten** — ntfy-Priorität separat für Warning- und Critical-Alerts setzen (1 min/lautlos … 5 dringend/bypass DND); dieselbe Skala nutzt auch der Quiet-Hours-Mindestschwellwert
- **Alert-Cooldown** — Verhindert Spam bei anhaltenden Problemen
- **Persistenter State** — versionierte Snapshot-Historie pro Backup-Gruppe
- **Benachrichtigungs-History-Log** — jeder versendete Alert (inkl. Test-Benachrichtigungen aus der Web UI) wird an `notification_log.json` angehängt; über das 📋 Log-Panel der Web UI einsehbar und löschbar

### Setup

```bash
cd alerting
pip install -r requirements.txt

# Initiale Konfiguration erstellen (optional)
cp config.json.example config.json
```

### Konfiguration

**Option 1: Über die Web UI (empfohlen bei Verwendung beider Tools)**

Wenn du das Web UI Tool verwendest (siehe oben), kannst du alle Alerting-Einstellungen über die Weboberfläche konfigurieren:

1. Web UI starten: `cd ../webui && python app.py`
2. [http://127.0.0.1:5111](http://127.0.0.1:5111) öffnen
3. Zahnrad-Symbol (⚙️) klicken um **Alerting-Konfiguration** zu öffnen
4. Push-Benachrichtigungen konfigurieren:
   - **ntfy Topic**: Topic-Name eingeben (z.B. "meine-pbs-alerts") um Benachrichtigungen zu aktivieren
   - **ntfy URL**: Meist `https://ntfy.sh` (Standard)
   - **ntfy Token**: Optional, für private ntfy-Instanzen
5. Alert-Prioritäten unter **Notifications → Alert Priorities** festlegen (Warning = 4 high, Critical = 5 urgent als Standard). Dieselbe Skala 1–5 gilt auch für das Feld **Minimum priority to send** unter Quiet Hours.
6. Weitere Einstellungen nach Bedarf anpassen (Schwellwerte, Ruhezeiten, Daemon-Intervall, etc.)
7. Einstellungen speichern

**Option 2: Manuelle Bearbeitung der Konfigurationsdatei**

Alternativ die `alerting/config.json` direkt bearbeiten. Alle verfügbaren Parameter sind in der [Konfigurationsdatei-Referenz](#konfigurationsdatei-referenz) weiter unten beschrieben:

```bash
cp config.json.example config.json
# alerting/config.json bearbeiten und mindestens API-Key und (optional) ntfy_topic setzen
```

### Nutzung

```bash
# Einmaliger Check
python monitor.py

# Daemon-Modus (alle 30 Minuten)
python monitor.py --daemon 1800
```

### Cron-Job (empfohlen)

```bash
# Alle 30 Minuten prüfen
*/30 * * * * cd /path/to/PBS_monitor/alerting && /usr/bin/python3 monitor.py >> /var/log/pbs-monitor.log 2>&1
```

### Konfigurationsdatei-Referenz

Bei manueller Konfiguration (Option 2 oben) wird die Datei `alerting/config.json` beim ersten Start automatisch aus `alerting/config.json.example` kopiert, falls sie noch nicht existiert. Du kannst sie jedoch auch vorab manuell erstellen:

```json
{
  "_comment_api": "Base URL of the Monitoring API",
  "api_base": "https://api.remote-backups.com",
  
  "_comment_ntfy": "Push-Benachrichtigungseinstellungen. ntfy_topic konfigurieren um externe Benachrichtigungen zu aktivieren. Leer lassen zum Deaktivieren.",
  "ntfy_url": "https://ntfy.sh",
  "ntfy_topic": "",
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
  
  "_comment_priorities": "ntfy-Priorit\u00e4t f\u00fcr Warning- (4=high) und Critical-Alerts (5=urgent). Bereich 1\u20135. Stufe 5 umgeht Do Not Disturb auf unterst\u00fctzten Ger\u00e4ten. Das Feld min_priority in quiet_hours nutzt dieselbe Skala.",
  "notification_priorities": {
    "warning": 4,
    "critical": 5
  },
  
  "_comment_learning": "Toggles dynamic learning for missed backup window detection.",
  "schedule_learning": {
    "enabled": true,
    "timezone": "local",
    "history_window_days": 60,
    "min_occurrences": 2,
    "time_tolerance_minutes": 30,
    "due_grace_minutes": 30,
    "stale_after_days": 8,
    "snapshot_retention_count": 24
  },
  
  "_comment_cooldown": "Minimum minutes to wait before repeating an alert of the same type.",
  "alert_cooldown_minutes": 60,
  
  "_comment_daemon": "Interval for daemon mode checks in seconds.",
  "daemon_interval_seconds": 1800
}
```

Manuelle und gesperrte Gruppen-Schedules werden separat in `alerting/group_rules.json` gespeichert.
Unterstützte manuelle Schedule-Typen sind `daily`, `weekly` und `interval`.

| Parameter | Beschreibung |
|-----------|-------------|
| `ntfy_topic` | **Konfigurieren um Push-Benachrichtigungen zu aktivieren** (z.B. "meine-alerts"). Leer lassen um externe Benachrichtigungen zu deaktivieren |
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
| `schedule_learning.snapshot_retention_count` | Wie viele der neuesten Snapshots pro Backup-Gruppe im State gespeichert werden (Standard: 24). Wird für Schedule-Learning und Snapshot-Verlust-Erkennung verwendet — bei Gruppen mit vielen täglichen Backups erhöhen |
| `alert_cooldown_minutes` | Mindestzeit zwischen wiederholten Alerts gleichen Typs |
| `daemon_interval_seconds` | Wie oft der Daemon nach Problemen sucht im Daemon-Modus (`--daemon` oder Docker-Container, Sekunden, Standard: 1800). In der Web UI unter **Daemon Interval (minutes)** konfigurierbar — die Umrechnung erfolgt automatisch. |

Folgende Werte können auch als Umgebungsvariablen gesetzt werden (in `.env` oder der Shell):

| Umgebungsvariable | Beschreibung |
|-------------------|--------------|
| `ALERTING_DATA_DIR` | Überschreibt das Verzeichnis, in dem `config.json`, `state.json` und `group_rules.json` gespeichert werden. Standard: das `alerting/`-Verzeichnis des Scripts. In Docker-Containern wird dieser Wert automatisch gesetzt (`/app/data`). |
| `NTFY_TOKEN` | Überschreibt den `ntfy_token` aus `config.json`. Dieser Weg ist dem Speichern des Tokens auf der Festplatte vorzuziehen (z. B. `NTFY_TOKEN=tk_yoursecrettoken`). |

### Alert-Prioritäten (ntfy)

| Prio | Verwendung |
|------|-----------|
| 5 (urgent) | Storage ≥ 90%, Verification failed, alle Backups weg, API nicht erreichbar |
| 4 (high) | GC failed, Host offline, verpasstes Backup-Fenster oder Intervall, veraltete Replication, Immutable Disable pending, Snapshots unerwartet entfernt |
| 3 (default) | Storage ≥ 80%, GC/Verification überfällig oder nie gelaufen |

💡 **Tipp:** Alle diese Parameter können einfach über die Web-UI konfiguriert werden, anstatt die JSON-Dateien manuell zu bearbeiten.

---

## API-Limitierungen

Die Monitoring API ist read-only. Sie liefert inzwischen live PBS-Namespaces, Backup-Gruppen und Snapshots, aber Folgendes ist darüber weiterhin **nicht** verfügbar:

- Ob ein Snapshot aus einem automatischen oder manuellen Lauf stammt
- Per-Snapshot Verification-Status
- Konfigurierte Backup-Schedules oder -Frequenzen pro Gruppe
- I/O-Graphen oder langfristige Zeitreihen-Daten

Das Alerting persistiert deshalb jetzt die Backup-Browser-Daten pro Namespace und Gruppe und lernt daraus konservative Wochentag-/Zeit-Slots oder kurze Intervalle. Aktuell kann das Backup-Alerting erkennen:
- ✅ Ob alle sichtbaren PBS-Backups verschwunden sind
- ✅ Ob die Snapshot-Anzahl einer Gruppe unter die `keep_last`-Policy fällt (unerwartete Löschung)
- ✅ Ob ein gelerntes wiederkehrendes Backup-Fenster für eine bestimmte Backup-Gruppe verpasst wurde
- ✅ Häufige wiederkehrende Backups wie alle 2 Stunden per Intervall-Erkennung — mit optionaler fester Ankerzeit für ausgerichtete Slot-Erkennung (z. B. `06:00` + alle 2 h → 06:00, 08:00, 10:00 …)
- ✅ Täglich wiederkehrende Backups als eigener editierbarer Schedule-Typ
- ✅ Off-Schedule-Snapshots am selben Tag als Kontext, ohne sie als Beweis für einen erfolgreichen geplanten Lauf zu werten
- ❌ Komplexere Rhythmen wie monatliche, zweiwöchentliche oder wirklich unregelmäßige Schedules

### Genutzte Endpoints

| Endpoint | Auth | Beschreibung |
|----------|------|-------------|
| `GET /monitoring/v1/datastores` | Bearer | Alle Datastores mit Live-Metriken |
| `GET /monitoring/v1/datastores/{id}` | Bearer | Detail inkl. Prune, Autoscaling, Replication |
| `GET /monitoring/v1/datastores/{id}/backups` | Bearer | Namespace-aware PBS-Backup-Inventar |
| `GET /monitoring/v1/datastores/{id}/backups/rsync` | Bearer | rsync-Backup-Daten (Web UI) |
| `GET /monitoring/v1/datastores/{id}/backups/sftp` | Bearer | SFTP-Backup-Daten (Web UI) |
| `GET /monitoring/v1/datastores/{id}/backups/zfs-recv` | Bearer | ZFS-recv-Backup-Daten (Web UI) |
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
├── LICENSE
├── README.md                       # Englische Dokumentation
├── README_DE.md                    # Deutsche Dokumentation
├── .github/
│   └── workflows/
│       ├── ci.yml                  # CI: Lint, Tests, Docker-Smoke-Test
│       └── docker-publish.yml      # CI/CD: Docker-Images bauen und veröffentlichen
├── docker/                         # Docker-Deployment-Dateien
│   ├── quick-deploy.sh             # Ein-Befehl-Deploy-Script
│   ├── alerting/
│   │   └── Dockerfile
│   └── webui/
│       └── Dockerfile
├── tests/                          # Automatisierte Tests
│   ├── conftest.py
│   ├── test_auth.py
│   ├── test_csrf.py
│   ├── test_input_validation.py
│   ├── test_secret_redaction.py
│   ├── test_security_headers.py
│   ├── test_ssrf.py
│   └── requirements.txt
├── webui/                          # Tool 1: Web Dashboard
│   ├── app.py                      # Flask Server (Routen, Session-Handling)
│   ├── alerting_ui.py              # Alerting-bezogene UI-Routen und Hilfsfunktionen
│   ├── normalizers.py              # Eingabe-Normalisierungshilfsfunktionen
│   ├── validators.py               # Eingabeprüfung und SSRF-Schutz
│   ├── requirements.txt
│   ├── static/
│   │   └── js/
│   │       └── dashboard.js        # Dashboard-JavaScript
│   └── templates/
│       ├── index.html              # Single-Page Dashboard
│       └── login.html              # Login-Seite (bei gesetztem WEBUI_PASSWORD)
└── alerting/                       # Tool 2: Monitoring + Alerting
    ├── monitor.py                  # Haupt-Monitoring-Script (Einstiegspunkt)
    ├── normalization.py            # Datennormalisierungs-Hilfsfunktionen
    ├── notification.py             # ntfy-Benachrichtigungs-Versand
    ├── schedule.py                 # Schedule-Learning und Missed-Backup-Erkennung
    ├── requirements.txt
    ├── config.json.example         # Vorlage für Alerting-Konfiguration
    ├── config.json                 # Lokale Konfig (gitignored)
    ├── group_rules.json            # Lokale Gruppenregeln (gitignored, auto-generiert)
    ├── state.json                  # Runtime-State (gitignored, auto-generiert)
    └── notification_log.json       # Benachrichtigungs-History (gitignored, auto-generiert)
```

---

## Kontakt

Bei Fragen, Anregungen oder Problemen mit diesem Projekt kannst du mich gern kontaktieren:

📧 **E-Mail:** [maschkef-git@pm.me](mailto:maschkef-git@pm.me)

---

## Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert – siehe die [LICENSE](LICENSE) Datei für Details.
