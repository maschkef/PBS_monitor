#!/bin/bash
# Entrypoint for the pbs-test container.
#
# PBS normally runs under systemd. In a container we start the two API
# daemons directly in the foreground and wait for either to exit — Docker
# then restarts the container.
set -eu

# proxmox-backup-server installs the daemons under the Debian multiarch
# libdir (e.g. /usr/lib/x86_64-linux-gnu/proxmox-backup/), which is not on
# PATH by default. Glob to stay architecture-agnostic.
for d in /usr/lib/*/proxmox-backup; do
    [ -d "$d" ] && PATH="$d:$PATH"
done
export PATH

proxmox-backup-api &
api_pid=$!

proxmox-backup-proxy &
proxy_pid=$!

# Exit as soon as either daemon dies so Docker's restart policy applies.
wait -n "$api_pid" "$proxy_pid"
