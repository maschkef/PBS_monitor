#!/usr/bin/env bash
# Initialise the pbs-test container for CI smoke runs and the local
# docker-test/ compose harness.
#
# Idempotent: safe to re-run. On success:
#   - Writes PBS_TEST_TOKEN=... to ${PBS_SMOKE_OUT}/.pbs-token (env-file format).
#   - Renders ${PBS_SMOKE_OUT}/config.json from this directory's config.pbs.json,
#     substituting the live PBS cert fingerprint for the __PBS_FINGERPRINT__
#     placeholder.
#
# Environment:
#   PBS_CONTAINER        container name/ID of the pbs-test instance
#                        (default: "pbs-test-server").
#   PBS_SMOKE_OUT        output directory for rendered artefacts
#                        (default: "${PWD}/config" — matches the local
#                        docker-test/ workflow when invoked from docker-test/).
#   WITH_SAMPLE_BACKUPS  if "1", push a trivial example backup after setup.
#                        Default off — adds ~30 s to CI, only useful locally.
set -euo pipefail

PBS_CONTAINER="${PBS_CONTAINER:-pbs-test-server}"
WITH_SAMPLE_BACKUPS="${WITH_SAMPLE_BACKUPS:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PBS_SMOKE_OUT="${PBS_SMOKE_OUT:-${PWD}/config}"
TOKEN_FILE="${PBS_SMOKE_OUT}/.pbs-token"
CONFIG_TEMPLATE="${SCRIPT_DIR}/config.pbs.json"
CONFIG_RENDERED="${PBS_SMOKE_OUT}/config.json"

STORE_NAME="test-store"
STORE_PATH="/var/lib/pbs-test-store"
USER_ID="monitor@pbs"
USER_PASSWORD="changeme-dummy-not-for-login"   # token-only user; password unused
TOKEN_ID="smoke"

msg() { printf '\n[setup-pbs] %s\n' "$*" >&2; }

# ── 1. Wait for API ───────────────────────────────────────────────────────────
msg "Waiting for PBS API to come up inside ${PBS_CONTAINER}..."
for i in $(seq 1 60); do
    if docker exec "${PBS_CONTAINER}" curl -fsSk \
            https://localhost:8007/api2/json/version >/dev/null 2>&1; then
        msg "PBS API reachable after ${i}s"
        break
    fi
    if [ "$i" = "60" ]; then
        msg "ERROR: PBS API never became reachable within 60s"
        docker logs --tail=50 "${PBS_CONTAINER}" >&2 || true
        exit 1
    fi
    sleep 1
done

# ── 2. Datastore (idempotent) ─────────────────────────────────────────────────
msg "Ensuring datastore '${STORE_NAME}' exists..."
docker exec "${PBS_CONTAINER}" sh -c "
    mkdir -p '${STORE_PATH}' && \
    proxmox-backup-manager datastore list --output-format=json 2>/dev/null \
      | grep -q '\"name\": *\"${STORE_NAME}\"' \
    || proxmox-backup-manager datastore create '${STORE_NAME}' '${STORE_PATH}'
"

# ── 3. Monitor user (idempotent) ──────────────────────────────────────────────
msg "Ensuring user '${USER_ID}' exists..."
docker exec "${PBS_CONTAINER}" sh -c "
    proxmox-backup-manager user list --output-format=json 2>/dev/null \
      | grep -q '\"userid\": *\"${USER_ID}\"' \
    || proxmox-backup-manager user create '${USER_ID}' --password '${USER_PASSWORD}'
"

# ── 4. API Token — rotate on every run so the secret is fresh ─────────────────
msg "Rotating API token '${TOKEN_ID}' for '${USER_ID}'..."
docker exec "${PBS_CONTAINER}" proxmox-backup-manager user delete-token \
    "${USER_ID}" "${TOKEN_ID}" >/dev/null 2>&1 || true

TOKEN_JSON="$(docker exec "${PBS_CONTAINER}" \
    proxmox-backup-manager user generate-token \
    "${USER_ID}" "${TOKEN_ID}" --output-format=json)"

TOKEN_VALUE="$(printf '%s' "${TOKEN_JSON}" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d['tokenid']}:{d['value']}\")" )"

if [ -z "${TOKEN_VALUE}" ]; then
    msg "ERROR: failed to parse token value from generate-token output"
    printf '%s\n' "${TOKEN_JSON}" >&2
    exit 1
fi

# ── 5. ACL (idempotent) ───────────────────────────────────────────────────────
msg "Granting Audit on / with propagate..."
AUTH_ID="${USER_ID}!${TOKEN_ID}"
docker exec "${PBS_CONTAINER}" sh -c "
    proxmox-backup-manager acl update / Audit --auth-id '${AUTH_ID}' --propagate 1
"

# ── 6. Optional sample backup ─────────────────────────────────────────────────
if [ "${WITH_SAMPLE_BACKUPS}" = "1" ]; then
    msg "Creating a sample backup via proxmox-backup-client..."
    docker exec -e PBS_REPOSITORY="${AUTH_ID}@localhost:${STORE_NAME}" \
                -e PBS_PASSWORD="${TOKEN_VALUE#*:}" \
                -e PBS_FINGERPRINT="$(docker exec "${PBS_CONTAINER}" \
                    proxmox-backup-manager cert info --output-format=json 2>/dev/null \
                    | python3 -c "import json,sys; print(json.load(sys.stdin).get('fingerprint',''))" )" \
                "${PBS_CONTAINER}" sh -c "
        echo 'hello from docker-test' > /tmp/sample.txt && \
        proxmox-backup-client backup sample.txt:/tmp/sample.txt \
            --backup-type host --backup-id docker-test
    " || msg "WARNING: sample backup failed (continuing)."
fi

# ── 7. Persist token + render config.json ─────────────────────────────────────
mkdir -p "${PBS_SMOKE_OUT}"
printf 'PBS_TEST_TOKEN=%s\n' "${TOKEN_VALUE}" > "${TOKEN_FILE}"
chmod 600 "${TOKEN_FILE}"

# The alerting config requires fingerprint pinning whenever verify_tls=false.
# PBS's self-signed cert changes on every container rebuild, so we fetch it
# live and substitute it into the template.
msg "Extracting PBS cert SHA-256 fingerprint..."
FINGERPRINT="$(docker exec "${PBS_CONTAINER}" \
    proxmox-backup-manager cert info --output-format=json 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('fingerprint',''))")"

if [ -z "${FINGERPRINT}" ]; then
    msg "ERROR: failed to read cert fingerprint from PBS container"
    exit 1
fi

if [ -f "${CONFIG_TEMPLATE}" ]; then
    msg "Rendering ${CONFIG_RENDERED} with live fingerprint..."
    python3 - "${CONFIG_TEMPLATE}" "${CONFIG_RENDERED}" "${FINGERPRINT}" <<'PY'
import json, sys
src, dst, fp = sys.argv[1], sys.argv[2], sys.argv[3]
with open(src) as f:
    cfg = json.load(f)
cfg.pop("_comment", None)
for entry in cfg.get("sources", []):
    if entry.get("type") == "pbs" and entry.get("fingerprint") == "__PBS_FINGERPRINT__":
        entry["fingerprint"] = fp
with open(dst, "w") as f:
    json.dump(cfg, f, indent=2)
    f.write("\n")
PY
else
    msg "NOTE: ${CONFIG_TEMPLATE} not found; skipped rendering config.json"
fi

msg "Done. Token written to ${TOKEN_FILE}"
msg "       Fingerprint: ${FINGERPRINT}"
msg "       Config:      ${CONFIG_RENDERED}"
msg "Next: 'docker compose exec pbs-alerting python -m alerting.monitor'"
