#!/bin/sh
set -e

# When .env does not exist on the host before the container starts, Docker
# creates a directory at the bind-mount path instead of a file. That directory
# becomes a mount point inside the container and cannot be removed or replaced.
# The only safe action is to abort with a clear message.
if [ -d /app/.env ]; then
    echo ""
    echo "ERROR: /app/.env is a directory, not a file."
    echo ""
    echo "This happens when .env does not exist on the host before starting"
    echo "the containers. Docker created a directory as the bind-mount target."
    echo ""
    echo "Fix (run on the host in your compose directory):"
    echo "  docker compose down"
    echo "  sudo rm -rf .env"
    echo "  cp .env.example .env            # if you have .env.example locally"
    echo "  # -- or download it directly:"
    echo "  curl -sLo .env https://raw.githubusercontent.com/maschkef/PBS_monitor/main/.env.example"
    echo "  # Edit .env and set API_KEY"
    echo "  docker compose up"
    echo ""
    exit 1
fi

# Fix ownership of the data directory so the pbs user can write to it.
# This is necessary because Docker creates bind-mounted host directories as
# root, even when the container runs as a non-root user.
chown -R pbs:pbs /app/data

# Drop privileges and exec the main process, replacing this shell.
exec gosu pbs "$@"

# Fix ownership of the data directory so the pbs user can write to it.
# This is necessary because Docker creates bind-mounted host directories as
# root, even when the container runs as a non-root user.
chown -R pbs:pbs /app/data

# Drop privileges and exec the main process, replacing this shell.
exec gosu pbs "$@"
