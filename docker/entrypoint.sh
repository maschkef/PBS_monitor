#!/bin/sh
set -e

# Fix ownership of the data directory so the pbs user can write to it.
# This is necessary because Docker creates bind-mounted host directories as
# root, even when the container runs as a non-root user.
chown -R pbs:pbs /app/data

# Drop privileges and exec the main process, replacing this shell.
exec gosu pbs "$@"
