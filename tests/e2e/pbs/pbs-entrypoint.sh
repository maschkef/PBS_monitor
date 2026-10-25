#!/bin/bash
# Entrypoint for the pbs-test container.
#
# PBS normally runs under systemd. In a container we start the two API
# daemons directly in the foreground and wait for either to exit — Docker
# then restarts the container.
set -eu

proxmox-backup-api &
api_pid=$!

proxmox-backup-proxy &
proxy_pid=$!

# Exit as soon as either daemon dies so Docker's restart policy applies.
wait -n "$api_pid" "$proxy_pid"
