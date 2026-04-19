#!/bin/sh
set -e

# If .env does not exist on the host, Docker bind-mount creates a directory
# instead of a file. Detect and replace it with an empty file so dotenv does
# not crash. Users should create .env (e.g. from .env.example) before starting.
if [ -d /app/.env ]; then
    rm -rf /app/.env
    touch /app/.env
    echo "[entrypoint] WARNING: /app/.env was a directory (no .env on host). Created empty file."
    echo "[entrypoint] Copy .env.example to .env and set API_KEY, then restart the container."
fi

# Fix ownership of the data directory so the pbs user can write to it.
# This is necessary because Docker creates bind-mounted host directories as
# root, even when the container runs as a non-root user.
chown -R pbs:pbs /app/data

# Drop privileges and exec the main process, replacing this shell.
exec gosu pbs "$@"
