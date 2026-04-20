"""Pure data-formatting and normalization helpers for the PBS Monitor Web UI.

All functions in this module are stateless and have no external dependencies
beyond the Python standard library.
"""

from datetime import datetime, timezone


def format_bytes(b):
    """Format bytes to human readable string (base-1000 / SI units)."""
    if b is None:
        return "N/A"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(b) < 1000:
            return f"{b:.1f} {unit}"
        b /= 1000
    return f"{b:.1f} PB"


def format_binary_bytes(b):
    """Format bytes using IEC units for technical backup browser data."""
    if b is None:
        return "N/A"
    if b == 0:
        return "0 B"
    value = float(b)
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PiB"


def unix_to_iso(timestamp):
    """Convert a UNIX timestamp to an ISO 8601 string."""
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def normalize_backup_file(file_entry):
    """Normalize file metadata returned by backup browsing."""
    size = file_entry.get("size")
    return {
        "filename": file_entry.get("filename", "unknown"),
        "size": size,
        "size_human": format_binary_bytes(size),
        "csum": file_entry.get("csum"),
    }


def normalize_backup_snapshot(snapshot):
    """Normalize a single PBS snapshot returned by backup browsing."""
    files = [normalize_backup_file(file_entry) for file_entry in snapshot.get("files") or []]
    backup_time = snapshot.get("backup_time")

    raw_verification = snapshot.get("verification")
    if isinstance(raw_verification, dict):
        verification_state = raw_verification.get("state")
    elif isinstance(raw_verification, str):
        verification_state = raw_verification
    else:
        verification_state = None

    return {
        "backup_type": snapshot.get("backup_type"),
        "backup_id": str(snapshot.get("backup_id", "")),
        "backup_time": backup_time,
        "backup_time_iso": unix_to_iso(backup_time),
        "size": snapshot.get("size"),
        "size_human": format_binary_bytes(snapshot.get("size")),
        "protected": bool(snapshot.get("protected", False)),
        "comment": snapshot.get("comment"),
        "file_count": len(files),
        "files": files,
        "verification_state": verification_state,
    }


def is_trivial_zfs_recv_entry(entry):
    """Return True for internal-looking ZFS receive metadata snapshots."""
    referenced_bytes = entry.get("referencedBytes")
    if referenced_bytes is None:
        return False
    return (
        entry.get("type") == "snapshot"
        and entry.get("usedBytes") == 0
        and entry.get("depth") == -1
        and referenced_bytes <= 131072
    )


def should_hide_zfs_recv(payload):
    """Suppress zfs-recv if it only contains trivial metadata entries."""
    return isinstance(payload, list) and payload and all(
        is_trivial_zfs_recv_entry(entry) for entry in payload
    )


def normalize_backup_group(group, snapshots):
    """Attach snapshots to a PBS backup group and add display helpers."""
    last_backup = group.get("last_backup")
    sorted_snapshots = sorted(
        snapshots,
        key=lambda item: item.get("backup_time") or 0,
        reverse=True,
    )
    latest_comment = next(
        (
            snapshot.get("comment")
            for snapshot in sorted_snapshots
            if snapshot.get("comment")
        ),
        None,
    )
    distinct_comments = []
    for snapshot in sorted_snapshots:
        comment = snapshot.get("comment")
        if comment and comment not in distinct_comments:
            distinct_comments.append(comment)

    return {
        "backup_type": group.get("backup_type"),
        "backup_id": str(group.get("backup_id", "")),
        "group_key": f"{group.get('backup_type', 'other')}/{group.get('backup_id', 'unknown')}",
        "display_name": latest_comment or group.get("comment"),
        "last_backup": last_backup,
        "last_backup_iso": unix_to_iso(last_backup),
        "backup_count": group.get("backup_count", len(sorted_snapshots)),
        "comment": group.get("comment"),
        "latest_comment": latest_comment,
        "distinct_comments": distinct_comments,
        "snapshot_count": len(sorted_snapshots),
        "snapshots": sorted_snapshots,
    }


def normalize_namespace(namespace_meta, namespace_data):
    """Normalize a namespace payload into a grouped browser structure."""
    snapshots_by_group = {}
    for snapshot in namespace_data.get("snapshots") or []:
        normalized_snapshot = normalize_backup_snapshot(snapshot)
        key = (normalized_snapshot["backup_type"], normalized_snapshot["backup_id"])
        snapshots_by_group.setdefault(key, []).append(normalized_snapshot)

    groups = []
    for group in namespace_data.get("groups") or []:
        key = (group.get("backup_type"), str(group.get("backup_id", "")))
        group_snapshots = snapshots_by_group.pop(key, [])
        groups.append(normalize_backup_group(group, group_snapshots))

    for (backup_type, backup_id), group_snapshots in snapshots_by_group.items():
        synthetic_group = {
            "backup_type": backup_type,
            "backup_id": backup_id,
            "last_backup": group_snapshots[0].get("backup_time") if group_snapshots else None,
            "backup_count": len(group_snapshots),
            "comment": None,
        }
        groups.append(normalize_backup_group(synthetic_group, group_snapshots))

    groups.sort(
        key=lambda item: (
            -(item.get("last_backup") or 0),
            item.get("backup_type") or "",
            item.get("backup_id") or "",
        )
    )

    namespace_value = namespace_meta.get("ns", "")
    return {
        "ns": namespace_value,
        "label": namespace_value or "root",
        "comment": namespace_meta.get("comment"),
        "group_count": len(groups),
        "snapshot_count": sum(group.get("snapshot_count", 0) for group in groups),
        "groups": groups,
    }


def time_ago(iso_str):
    """Convert ISO timestamp to human-readable 'time ago' string."""
    if not iso_str:
        return "never"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    diff = now - dt
    seconds = int(diff.total_seconds())
    if seconds < 0:
        return format_time_until(abs(seconds))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def format_time_until(seconds):
    """Format seconds until a future event."""
    if seconds < 3600:
        return f"in {seconds // 60}m"
    if seconds < 86400:
        return f"in {seconds // 3600}h"
    return f"in {seconds // 86400}d"


def time_until(iso_str):
    """Convert ISO timestamp to 'time until' string."""
    if not iso_str:
        return "N/A"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    diff = dt - now
    seconds = int(diff.total_seconds())
    if seconds < 0:
        return "overdue"
    return format_time_until(seconds)
