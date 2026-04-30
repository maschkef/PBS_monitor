#!/bin/sh
set -e

# Fix ownership of the data directory so the pbs user can write to it.
# This is necessary because Docker creates bind-mounted host directories as
# root, even when the container runs as a non-root user.
chown -R pbs:pbs /app/data

# Restrict permissions: data is private to the pbs user (no world access).
# Existing files on a host-mounted volume may have loose permissions from the
# host, so normalise them explicitly.
find /app/data -type d -exec chmod 750 {} +
find /app/data -type f -exec chmod 640 {} +

# Limit new file creation: owner rw, group r, other nothing (umask 027).
# exec inherits the umask, so files written at runtime (config.json,
# state.json, group_rules.json) will be created as 640 rather than 644.
umask 027

# Drop privileges and exec the main process, replacing this shell.
exec gosu pbs "$@"
