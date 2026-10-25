# Technical Documentation: Local PBS Source (`type: "pbs"`)

## Overview

PBS Monitor can watch backups on a **locally-reachable Proxmox Backup Server** in addition to (or instead of) the `remote-backups.com` cloud. Each monitored instance is a `source` in the alerting config and is driven by the `PbsAdapter` ([alerting/sources/pbs.py](../alerting/sources/pbs.py)) via the native PBS HTTP API.

The adapter speaks the PBS API directly over HTTPS using an **API token** (`PBSAPIToken`-style auth). No agent or SSH is required.

Multiple PBS sources can be configured side-by-side; each one appears as a separate entry under `config.sources[]`.

## Configuration reference

A PBS source lives inside the `sources` list in `config.json`:

```jsonc
{
  "sources": [
    {
      "id": "home-lab",
      "type": "pbs",
      "label": "Home PBS",
      "base_url": "https://pbs.home.lan:8007",
      "node": "localhost",
      "token_env": "PBS_HOMELAB_TOKEN",
      "verify_tls": true,
      "fingerprint": null,
      "datastores": []
    }
  ]
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | yes | — | Slug matching `^[a-z0-9][a-z0-9-]*$`. Must be unique across all sources. Appears in each datastore ID as `pbs:<id>:<store>`. |
| `type` | yes | — | Must be `"pbs"`. |
| `label` | no | `null` | Human-readable name shown in the UI badge. Falls back to the datastore name when unset. |
| `base_url` | yes | — | Full URL to the PBS web interface, including scheme and port (default PBS port is `8007`). |
| `node` | yes | `"localhost"` | PBS node name used for `/nodes/<node>/*` endpoints. `"localhost"` works on every PBS install (it is an alias for the real node name); set this explicitly only if your PBS rejects the alias. **Short hostname** — no FQDN, no URL. |
| `token_env` | yes | — | **Environment variable name** holding the full PBS token value (format `USER@REALM!TOKENID:SECRET`). The token itself must never be committed to config files. |
| `verify_tls` | no | `true` | Validate the server's TLS chain the normal way. Set to `false` **only** when `fingerprint` pinning is also configured. |
| `fingerprint` | no | `null` | SHA-256 fingerprint of the server certificate for self-signed deployments. 64 hex chars; colons are allowed. See [Fingerprint pinning](#fingerprint-pinning) below. |
| `datastores` | no | `[]` | Allow-list of PBS datastore names to monitor. Empty list = auto-discover all datastores the token can see. |

### Validation rules

Enforced by `validate_source_entries()` in [alerting/sources/registry.py](../alerting/sources/registry.py):

- `verify_tls: false` **requires** `fingerprint` to be set — the adapter never ships completely unverified TLS.
- `node` must be a short hostname; FQDNs or URLs are rejected.
- `token_env` must be a valid environment-variable name (`^[A-Za-z_][A-Za-z0-9_]*$`).
- `base_url` must start with `http://` or `https://`.
- Duplicate `id` values across sources raise `ValueError` at config-load time.

### Minimal (public-CA) example

If your PBS is reachable via a public hostname with a trusted certificate (Let's Encrypt etc.), the config stays short:

```json
{
  "id": "home",
  "type": "pbs",
  "base_url": "https://pbs.example.com:8007",
  "token_env": "PBS_HOME_TOKEN"
}
```

### Self-signed TLS example (fingerprint pinning)

For the default PBS self-signed certificate — never use `verify_tls: false` without a fingerprint:

```json
{
  "id": "home-lab",
  "type": "pbs",
  "base_url": "https://192.168.1.50:8007",
  "token_env": "PBS_HOMELAB_TOKEN",
  "verify_tls": false,
  "fingerprint": "EC:D4:1B:18:7C:DA:D8:15:EB:8A:23:B8:AA:55:BB:DE:F4:EC:4F:57:0C:7D:A3:40:CD:B4:34:A9:CB:3F:18:59"
}
```

## Token setup

### Create the token in PBS

1. Log into the PBS web UI as `root@pam`.
2. **Configuration → Access Control → API Token → Add**.
3. User: `root@pam` (or a dedicated user); Token ID: e.g. `monitor`; leave *Expire* empty.
4. Copy the generated secret — **it is shown only once**. The full token value for `.env` is:

   ```
   root@pam!monitor:12345678-1234-1234-1234-123456789abc
   ```

### Grant permissions

The token needs read-only visibility into the datastores and node. Grant **`Datastore.Audit`** and **`Sys.Audit`** on `/` with the *Propagate* flag set — enough for every endpoint the adapter touches. In the PBS UI: *Configuration → Access Control → Permissions → Add → API Token Permission*.

### Store the token in `.env`

Add one line per source, keyed by the `token_env` from your config:

```bash
PBS_HOMELAB_TOKEN=root@pam!monitor:12345678-1234-1234-1234-123456789abc
```

The variable name is **arbitrary** — anything that matches `^[A-Za-z_][A-Za-z0-9_]*$`. Use a scheme like `PBS_<SOURCE_ID>_TOKEN` so multiple PBS instances stay distinguishable.

> **Restart required.** Both the alerting daemon and the web UI read the environment once at startup. After changing `.env`, restart the containers (`docker compose restart`) or the local processes.

## Fingerprint pinning

Extract the fingerprint from the live PBS cert:

```bash
openssl s_client -connect pbs.example.com:8007 -servername pbs.example.com </dev/null 2>/dev/null \
  | openssl x509 -noout -fingerprint -sha256 \
  | sed 's/^SHA256 Fingerprint=//'
```

Paste the result (colons-separated or without colons) as the `fingerprint` field. The adapter verifies the server certificate against this value on every request; a mismatch raises `SourceConnectionError`.

## PBS API endpoints used by the adapter

| Endpoint | Called by | Purpose |
|---|---|---|
| `GET /api2/json/version` | `check_health` | Health probe + version string |
| `GET /api2/json/admin/datastore` | `fetch_datastores` (auto-discovery) | List datastore names |
| `GET /api2/json/admin/datastore/{store}/status` | `fetch_datastores` | Storage metrics (`used`, `avail`, `total`) |
| `GET /api2/json/admin/datastore/{store}/namespace` | `fetch_backup_inventory` | List namespaces |
| `GET /api2/json/admin/datastore/{store}/groups?ns={ns}` | `fetch_backup_inventory` | List backup groups per namespace |
| `GET /api2/json/admin/datastore/{store}/snapshots?ns={ns}` | `fetch_backup_inventory` | List snapshots per namespace |
| `GET /api2/json/nodes/{node}/tasks?typefilter=garbage_collection&store={store}` | `fetch_datastores` | Last GC task |
| `GET /api2/json/nodes/{node}/tasks?typefilter=verificationjob` | `fetch_datastores` | Last verify task (filtered on `worker_id` prefix) |
| `GET /api2/json/config/datastore/{store}` | `fetch_datastores` | `gc-schedule`, `verify-new` configuration |
| `GET /api2/json/config/verify` | `fetch_datastores` | Verification jobs for schedule + `outdated-after` |
| `GET /api2/json/config/prune` | `fetch_datastores` | Prune jobs for retention policy |

All requests use `Authorization: PBSAPIToken=<token>` and a 15 s timeout — identical to the Cloud adapter.

## Alert coverage per source type

Not every alert type makes sense for every source. The table shows which alerts can fire for each source type.

| Alert | `remote_backups` (Cloud) | `pbs` (local) | Notes |
|---|:---:|:---:|---|
| GC Failed / Overdue / Never Ran | ✅ | ✅ | Last-task status from `nodes/<node>/tasks?typefilter=garbage_collection` |
| Verification Failed / Overdue / Never Ran | ✅ | ✅ | `typefilter=verificationjob` + `outdated_after_days` from `/config/verify` |
| Storage Warning / Critical | ✅ | ✅ | From `/admin/datastore/<store>/status` |
| Missed Backup Window / Interval | ✅ | ✅ | Snapshot-history-driven; works for both since both return the same inventory shape |
| All Backups Gone / Snapshots Unexpectedly Removed | ✅ | ✅ | Same path |
| Replication Never Synced / Stale | ✅ | ❌ | PBS has no cloud-replication concept the adapter can read |
| Immutable Backup Disable Pending | ✅ | ❌ | Cloud-only feature |
| Host Offline (platform health) | ✅ | — | Cloud-only platform health channel |
| API Unreachable / Unhealthy | ✅ | ✅ | Any source going down fires a per-source alert |
| Ntfy Delivery Failed / Heartbeat Failed | ✅ | ✅ | Infrastructure alerts, source-independent |

Fields that are PBS-foreign (`autoscaling`, `rescale_history`, `immutable_backup`, `replication`) are **omitted** from the datastore dict rather than set to `None`; the UI panels for them are skipped automatically for PBS entries.

## Datastore ID format

Datastore IDs are globally unique across sources:

- Cloud datastores keep their MongoDB `_id` (e.g. `"68a3b2…"`).
- PBS datastores are reported as `pbs:<source_id>:<store>` — e.g. `pbs:home-lab:ssd`.

The prefix is parsed by the WebUI to route backup-browser requests to the right adapter; do not rename sources casually once state has been persisted.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `check_health` → `degraded`, message mentions env var | `token_env` points at an undefined environment variable | Add the variable to `.env` and restart |
| `check_health` → `down`, message `TLS verification failed` | `fingerprint` is wrong or expired, or `verify_tls: true` against a self-signed cert | Re-extract fingerprint via `openssl s_client` |
| `check_health` → `degraded` with `401` | Token revoked or lacking permissions | Re-grant `Datastore.Audit` + `Sys.Audit` on `/` with Propagate |
| `fetch_datastores` returns an empty list | Token has no `Datastore.Audit` permission on any store, or no store exists | Check PBS ACL; create a datastore |
| Monitor runs but PBS datastores never appear in `/api/datastores` | Config lives in the file but the `token_env` secret is missing — the adapter raises `RuntimeError` on first use | Set the token in `.env` and restart |

## Related

- [docs/architecture.md](architecture.md) — multi-source adapter pattern overview
- [docs/alerting_monitor.md](alerting_monitor.md) — how alerts are composed per source
- [alerting/sources/registry.py](../alerting/sources/registry.py) — config validation + adapter factory
- [alerting/sources/pbs.py](../alerting/sources/pbs.py) — PBS adapter implementation
- [tests/test_source_pbs.py](../tests/test_source_pbs.py) — unit tests covering every endpoint
