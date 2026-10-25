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

# /run/proxmox-backup is normally created by systemd-tmpfiles — do it manually
# since we have no systemd here. The datastore root (bind-mounted tmpfs at
# container start) also needs backup ownership so the proxy can write into it.
install -d -m 0755 -o backup -g backup /run/proxmox-backup
chown backup:backup /var/lib/pbs-test-store

# proxmox-backup-api runs as root (it handles privileged setup internally).
proxmox-backup-api &
api_pid=$!

# proxmox-backup-proxy refuses to start as root — drop to the `backup` user,
# matching the upstream proxmox-backup-proxy.service unit.
runuser -u backup -g backup -- proxmox-backup-proxy &
proxy_pid=$!

# Exit as soon as either daemon dies so Docker's restart policy applies.
wait -n "$api_pid" "$proxy_pid"
