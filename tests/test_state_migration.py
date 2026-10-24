"""Tests for additive source_type/source_id state-schema fields (Phase 2)."""
from alerting.normalization import default_datastore_state, migrate_state


def test_default_datastore_state_has_source_defaults():
    ds = default_datastore_state("x")
    assert ds["source_type"] == "remote_backups"
    assert ds["source_id"] == "default"


def test_migrate_state_adds_source_fields_when_missing():
    raw = {
        "version": 2,
        "datastores": {
            "abc": {"name": "my-ds"},
        },
    }
    migrated = migrate_state(raw)
    assert migrated["datastores"]["abc"]["source_type"] == "remote_backups"
    assert migrated["datastores"]["abc"]["source_id"] == "default"


def test_migrate_state_preserves_existing_source_fields():
    raw = {
        "version": 2,
        "datastores": {
            "pbs:home-lab:ssd": {
                "name": "ssd",
                "source_type": "pbs",
                "source_id": "home-lab",
            },
        },
    }
    migrated = migrate_state(raw)
    ds = migrated["datastores"]["pbs:home-lab:ssd"]
    assert ds["source_type"] == "pbs"
    assert ds["source_id"] == "home-lab"


def test_migrate_state_ignores_non_string_source_fields():
    raw = {
        "version": 2,
        "datastores": {
            "abc": {
                "name": "my-ds",
                "source_type": None,
                "source_id": 42,
            },
        },
    }
    migrated = migrate_state(raw)
    assert migrated["datastores"]["abc"]["source_type"] == "remote_backups"
    assert migrated["datastores"]["abc"]["source_id"] == "default"
