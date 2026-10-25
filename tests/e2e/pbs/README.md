# PBS End-to-End Test Fixture

Files in this directory bootstrap a self-contained Proxmox Backup Server
instance that the `pbs-smoke-test` CI job (see
[.github/workflows/ci.yml](../../../.github/workflows/ci.yml)) uses on every PR
to exercise the native PBS source adapter against a real `proxmox-backup-proxy`.

The same fixtures are also used by the maintainer's local pre-release Compose
harness in `docker-test/` — that harness is intentionally not shipped (local
scratch area, see [.gitignore](../../../.gitignore)), but it mounts the files
here via relative paths.

This is **not a pytest target.** `pytest tests/` skips this directory because it
contains no `test_*.py` modules. In-process unit coverage for the PBS adapter
lives in [tests/test_source_pbs.py](../../test_source_pbs.py).

## Files

| File | Role |
| --- | --- |
| `Dockerfile.pbs` | Debian + `pbs-no-subscription` apt repo; builds a minimal `proxmox-backup-server` image. |
| `pbs-entrypoint.sh` | Starts `proxmox-backup-api` and `proxmox-backup-proxy` in the foreground; exits when either dies so Docker can restart cleanly. |
| `setup-pbs.sh` | Idempotent initialiser. Creates the datastore + user + API token, grants Audit ACLs, extracts the live cert fingerprint, writes `.pbs-token` and `config.json` to `${PBS_SMOKE_OUT}` (default `${PWD}/config`). |
| `config.pbs.json` | Template config with both a `remote_backups` and a `pbs` source. The `__PBS_FINGERPRINT__` placeholder is substituted by `setup-pbs.sh`. |

## Running the fixture by hand

```bash
# Build and start the PBS container from the repo root:
docker build -f tests/e2e/pbs/Dockerfile.pbs -t pbs-test-server:local tests/e2e/pbs
docker run -d --name pbs-test-server --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined --tmpfs /var/lib/pbs-test-store \
    -p 8007:8007 pbs-test-server:local

# Initialise and emit artefacts to /tmp/pbs-out/:
PBS_SMOKE_OUT=/tmp/pbs-out bash tests/e2e/pbs/setup-pbs.sh
cat /tmp/pbs-out/.pbs-token
cat /tmp/pbs-out/config.json
```

### Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `PBS_CONTAINER` | `pbs-test-server` | Container name/ID the setup script `docker exec`s into. |
| `PBS_SMOKE_OUT` | `${PWD}/config` | Output directory for `.pbs-token` and rendered `config.json`. |
| `WITH_SAMPLE_BACKUPS` | `0` | If `1`, push a trivial example backup after setup (~30 s extra). |
