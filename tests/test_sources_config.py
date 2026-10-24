"""Tests for the Phase 3 source-config validator and factory.

Covers:
- `validate_source_entries` input shapes (empty, invalid, duplicate ids,
  unknown type, remote_backups required fields, pbs placeholder, api_key_env
  defaulting).
- `build_source` wiring of `api_key_env` into `RemoteBackupsAdapter` and the
  explicit Phase-4 refusal for `type="pbs"`.
"""

import pytest
import requests

from alerting.sources import (
    RemoteBackupsAdapter,
    build_source,
    validate_source_entries,
)


class TestValidateSourceEntries:
    def test_empty_list_returns_synthetic_default_with_legacy_api_base(self):
        result = validate_source_entries(
            [], legacy_api_base="https://api.example.com",
        )
        assert result == [{
            "id": "default",
            "type": "remote_backups",
            "api_base": "https://api.example.com",
            "api_key_env": "API_KEY",
        }]

    def test_none_list_returns_synthetic_default(self):
        result = validate_source_entries(
            None, legacy_api_base="https://api.example.com",
        )
        assert result[0]["id"] == "default"
        assert result[0]["type"] == "remote_backups"

    def test_empty_list_without_legacy_api_base_raises(self):
        with pytest.raises(ValueError, match="no sources configured"):
            validate_source_entries([], legacy_api_base=None)
        with pytest.raises(ValueError, match="no sources configured"):
            validate_source_entries([], legacy_api_base="")

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="must be a list"):
            validate_source_entries("not-a-list", legacy_api_base="x")

    def test_entry_not_dict_raises(self):
        with pytest.raises(ValueError, match=r"sources\[0\] must be an object"):
            validate_source_entries(["bad"], legacy_api_base="x")

    @pytest.mark.parametrize("bad_id", ["BadID", "-leading-dash", "", "has space", "with_underscore"])
    def test_bad_id_pattern_raises(self, bad_id):
        with pytest.raises(ValueError, match="id must match"):
            validate_source_entries(
                [{"id": bad_id, "type": "remote_backups", "api_base": "https://x"}],
                legacy_api_base="x",
            )

    def test_duplicate_id_raises(self):
        with pytest.raises(ValueError, match="duplicates earlier id"):
            validate_source_entries(
                [
                    {"id": "a", "type": "remote_backups", "api_base": "https://x"},
                    {"id": "a", "type": "remote_backups", "api_base": "https://y"},
                ],
                legacy_api_base="x",
            )

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="type must be one of"):
            validate_source_entries(
                [{"id": "a", "type": "weird", "api_base": "https://x"}],
                legacy_api_base="x",
            )

    def test_pbs_type_passes_validator_without_further_fields(self):
        # Phase 3 accepts pbs entries as-is; Phase 4 adds field validation.
        result = validate_source_entries(
            [{"id": "home", "type": "pbs"}],
            legacy_api_base="x",
        )
        assert result == [{"id": "home", "type": "pbs"}]

    def test_remote_backups_missing_api_base_raises(self):
        with pytest.raises(ValueError, match="api_base must be a non-empty string"):
            validate_source_entries(
                [{"id": "a", "type": "remote_backups"}],
                legacy_api_base="x",
            )
        with pytest.raises(ValueError, match="api_base must be a non-empty string"):
            validate_source_entries(
                [{"id": "a", "type": "remote_backups", "api_base": ""}],
                legacy_api_base="x",
            )

    def test_api_key_env_defaults_to_API_KEY(self):
        result = validate_source_entries(
            [{"id": "a", "type": "remote_backups", "api_base": "https://x"}],
            legacy_api_base="x",
        )
        assert result[0]["api_key_env"] == "API_KEY"

    def test_api_key_env_preserved_when_set(self):
        result = validate_source_entries(
            [{
                "id": "a", "type": "remote_backups",
                "api_base": "https://x", "api_key_env": "CUSTOM_KEY",
            }],
            legacy_api_base="x",
        )
        assert result[0]["api_key_env"] == "CUSTOM_KEY"

    def test_two_valid_entries_round_trip(self):
        entries = [
            {"id": "cloud-1", "type": "remote_backups", "api_base": "https://a"},
            {"id": "cloud-2", "type": "remote_backups", "api_base": "https://b"},
        ]
        result = validate_source_entries(entries, legacy_api_base="ignored")
        assert [e["id"] for e in result] == ["cloud-1", "cloud-2"]
        assert all(e["api_key_env"] == "API_KEY" for e in result)

    def test_does_not_mutate_input_entries(self):
        entries = [
            {"id": "a", "type": "remote_backups", "api_base": "https://x"},
        ]
        validate_source_entries(entries, legacy_api_base="x")
        assert "api_key_env" not in entries[0]


class TestBuildSource:
    def test_build_remote_backups_default_api_key_env(self):
        adapter = build_source({
            "id": "a", "type": "remote_backups", "api_base": "https://x",
        })
        assert isinstance(adapter, RemoteBackupsAdapter)
        assert adapter.source_id == "a"
        assert adapter._api_key_env == "API_KEY"

    def test_build_remote_backups_forwards_custom_api_key_env(self, monkeypatch):
        adapter = build_source({
            "id": "a", "type": "remote_backups",
            "api_base": "https://x", "api_key_env": "CLOUD2_TOKEN",
        })
        monkeypatch.setenv("CLOUD2_TOKEN", "shhh")

        captured = {}

        def fake_get(url, headers=None, params=None, timeout=None):
            captured["headers"] = headers

            class _Resp:
                def raise_for_status(self): pass
                def json(self): return []
            return _Resp()

        monkeypatch.setattr(requests, "get", fake_get)
        adapter._api_get("/dummy")
        assert captured["headers"]["Authorization"] == "Bearer shhh"

    def test_build_remote_backups_missing_env_var_raises_runtime(self, monkeypatch):
        monkeypatch.delenv("API_KEY", raising=False)
        adapter = build_source({
            "id": "a", "type": "remote_backups", "api_base": "https://x",
        })
        with pytest.raises(RuntimeError, match="API_KEY not set"):
            adapter._api_get("/dummy")

    def test_build_pbs_raises_in_phase3(self):
        with pytest.raises(ValueError, match="Phase 4"):
            build_source({"id": "home", "type": "pbs"})

    def test_build_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            build_source({"id": "a", "type": "weird"})
