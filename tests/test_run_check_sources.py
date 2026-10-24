"""Phase 3 end-to-end tests for the per-source iteration in `run_check()`.

Covers:
- `FakeAdapter` test double satisfies the `SourceAdapter` protocol.
- Legacy single-source config still drives one iteration.
- Multi-source config fans out to every entry.
- Platform-level alerts are keyed per source id (`platform:<id>:<title>`).
- A single `down`/`error` source does not abort the next source.
- The monitor-wide "Ntfy Delivery Failed" alert keeps its global key.
"""

from __future__ import annotations

import json

import pytest

from alerting import monitor as alert_monitor
from alerting.notification import NtfyDeliveryError
from alerting.sources import HealthResult, SourceAdapter, SourceConnectionError


class FakeAdapter:
    """Minimal in-memory SourceAdapter test double.

    Records every `fetch_datastores` call so tests can assert on fan-out.
    """

    def __init__(
        self,
        source_id: str,
        *,
        health: HealthResult | None = None,
        datastores: list[dict] | None = None,
    ) -> None:
        self._source_id = source_id
        self._health = health or HealthResult(status="ok", message="healthy")
        self._datastores = datastores or []
        self.fetch_datastores_calls = 0

    @property
    def source_id(self) -> str:
        return self._source_id

    def check_health(self) -> HealthResult:
        return self._health

    def fetch_datastores(self) -> list[dict]:
        self.fetch_datastores_calls += 1
        return list(self._datastores)

    def fetch_backup_inventory(self, datastore_id: str, *, snapshot_cap: int) -> dict:
        return {"namespaces": {}, "snapshot_count": 0}

    def fetch_rescale_log(self, datastore_id: str) -> list[dict]:
        return []


@pytest.fixture
def monitor_env(tmp_path, monkeypatch):
    """Point every monitor path at `tmp_path` and silence outbound HTTP.

    Returns a dict of knobs tests can poke:
      - `config`: the baseline config dict the test can mutate
      - `state`: a fresh default state
      - `ntfy_calls`: list that records every send_ntfy call
      - `register_adapters(entries)`: swap `build_source` for a lookup
         mapping each source cfg to a prepared `FakeAdapter`
    """
    monkeypatch.setattr(alert_monitor, "CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(alert_monitor, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        alert_monitor, "NOTIFICATION_LOG_PATH", tmp_path / "notification_log.json",
    )
    monkeypatch.setattr(
        alert_monitor, "GROUP_RULES_PATH", tmp_path / "group_rules.json",
    )

    ntfy_calls: list[dict] = []

    def _fake_send_ntfy(config, alert):
        ntfy_calls.append({
            "key": alert.key,
            "title": alert.title,
            "source_id": getattr(alert, "source_id", None),
            "display_title": getattr(alert, "display_title", alert.title),
        })
        return True

    monkeypatch.setattr(alert_monitor, "send_ntfy", _fake_send_ntfy)

    # Pin status-feed enrichment to a no-op so tests don't touch the network.
    monkeypatch.setattr(
        alert_monitor,
        "_enrich_alert_with_status_feed",
        lambda alert, config, event_time=None: None,
    )

    config = {
        **alert_monitor.DEFAULT_CONFIG,
        "ntfy_topic": "test-topic",
        "status_feed": {**alert_monitor.DEFAULT_CONFIG["status_feed"], "enabled": False},
        "heartbeat_url": "",
        "heartbeat_fail_url": "",
    }
    state = alert_monitor.default_state()

    def register_adapters(mapping: dict[str, FakeAdapter]):
        """Install a `build_source` stub that dispatches on `cfg_entry['id']`."""

        def _fake_build_source(cfg_entry):
            adapter = mapping.get(cfg_entry.get("id"))
            if adapter is None:
                raise AssertionError(
                    f"Unexpected build_source call for {cfg_entry!r}"
                )
            return adapter

        monkeypatch.setattr(alert_monitor, "build_source", _fake_build_source)

    return {
        "config": config,
        "state": state,
        "ntfy_calls": ntfy_calls,
        "register_adapters": register_adapters,
        "monkeypatch": monkeypatch,
        "tmp_path": tmp_path,
    }


class TestFakeAdapterProtocol:
    def test_fake_adapter_satisfies_source_adapter_protocol(self):
        adapter = FakeAdapter("x")
        assert isinstance(adapter, SourceAdapter)


class TestRunCheckSources:
    def test_single_source_default_still_works(self, monitor_env):
        """Load a legacy config (no `sources`) → run_check iterates exactly once."""
        env = monitor_env
        tmp_path = env["tmp_path"]
        (tmp_path / "config.json").write_text(json.dumps({
            "api_base": "https://api.example.com",
            "ntfy_topic": "x",
        }))

        config = alert_monitor.load_config()
        # Validator must have synthesised a single implicit source.
        assert [s["id"] for s in config["sources"]] == ["default"]

        adapter = FakeAdapter(
            "default",
            datastores=[{"id": "ds-1", "name": "DS 1", "metrics": {}}],
        )
        env["register_adapters"]({"default": adapter})

        state = alert_monitor.default_state()
        alert_monitor.run_check(config, state)

        assert adapter.fetch_datastores_calls == 1
        assert "ds-1" in state["datastores"]

    def test_two_cloud_sources_both_iterated(self, monitor_env):
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "cloud-a", "type": "remote_backups",
             "api_base": "https://a", "api_key_env": "API_KEY"},
            {"id": "cloud-b", "type": "remote_backups",
             "api_base": "https://b", "api_key_env": "API_KEY"},
        ]
        adapter_a = FakeAdapter(
            "cloud-a",
            datastores=[{"id": "ds-a", "name": "A", "metrics": {}}],
        )
        adapter_b = FakeAdapter(
            "cloud-b",
            datastores=[{"id": "ds-b", "name": "B", "metrics": {}}],
        )
        env["register_adapters"]({"cloud-a": adapter_a, "cloud-b": adapter_b})

        alert_monitor.run_check(env["config"], env["state"])

        assert adapter_a.fetch_datastores_calls == 1
        assert adapter_b.fetch_datastores_calls == 1
        assert "ds-a" in env["state"]["datastores"]
        assert "ds-b" in env["state"]["datastores"]

    def test_platform_health_alert_keyed_per_source_id(self, monitor_env):
        """Second source is `degraded` → `last_alerts` key is source-prefixed."""
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "cloud-a", "type": "remote_backups",
             "api_base": "https://a", "api_key_env": "API_KEY"},
            {"id": "cloud-b", "type": "remote_backups",
             "api_base": "https://b", "api_key_env": "API_KEY"},
        ]
        adapter_a = FakeAdapter("cloud-a")
        adapter_b = FakeAdapter(
            "cloud-b",
            health=HealthResult(status="degraded", message="partial outage"),
        )
        env["register_adapters"]({"cloud-a": adapter_a, "cloud-b": adapter_b})

        alert_monitor.run_check(env["config"], env["state"])

        keys = set(env["state"]["last_alerts"].keys())
        assert "platform:cloud-b:API Unhealthy" in keys
        # The pre-phase-3 unprefixed key must NOT appear — otherwise cooldown
        # state would silently collide across sources.
        assert "platform:API Unhealthy" not in keys
        # And the healthy source must not have produced a platform alert.
        assert not any(k.startswith("platform:cloud-a:") for k in keys)

    def test_source_down_does_not_abort_other_sources(self, monitor_env):
        """First source is `down` → second source is still processed.

        Covers the `return` → `continue` conversion inside the per-source loop.
        """
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "cloud-a", "type": "remote_backups",
             "api_base": "https://a", "api_key_env": "API_KEY"},
            {"id": "cloud-b", "type": "remote_backups",
             "api_base": "https://b", "api_key_env": "API_KEY"},
        ]
        adapter_a = FakeAdapter(
            "cloud-a",
            health=HealthResult(
                status="down", error=RuntimeError("boom"), message="unreachable",
            ),
        )
        adapter_b = FakeAdapter(
            "cloud-b",
            datastores=[{"id": "ds-b", "name": "B", "metrics": {}}],
        )
        env["register_adapters"]({"cloud-a": adapter_a, "cloud-b": adapter_b})

        alert_monitor.run_check(env["config"], env["state"])

        # Source A failed before fetch_datastores; source B still ran.
        assert adapter_a.fetch_datastores_calls == 0
        assert adapter_b.fetch_datastores_calls == 1
        assert "ds-b" in env["state"]["datastores"]
        assert "platform:cloud-a:API Unreachable" in env["state"]["last_alerts"]

    def test_ntfy_delivery_failed_key_is_global(self, monitor_env):
        """`send_ntfy` raising → monitor-wide `platform:Ntfy Delivery Failed` key.

        The Ntfy-delivery failure path lives outside the per-source loop, so
        its alert key stays unprefixed even in the multi-source world.
        """
        env = monitor_env
        monkeypatch = env["monkeypatch"]

        def _raise(config, alert):
            raise NtfyDeliveryError("ntfy down")

        monkeypatch.setattr(alert_monitor, "send_ntfy", _raise)
        # The "Ntfy Delivery Failed" branch only fires when at least one
        # heartbeat URL is configured; use a dummy and stub requests.get.
        env["config"]["heartbeat_url"] = "https://heartbeat.invalid/ping"
        env["config"]["sources"] = [
            {"id": "cloud-a", "type": "remote_backups",
             "api_base": "https://a", "api_key_env": "API_KEY"},
        ]
        # Degraded health → the source loop tries to send a platform alert →
        # safe_send_ntfy catches NtfyDeliveryError and flips the flag.
        adapter_a = FakeAdapter(
            "cloud-a",
            health=HealthResult(status="degraded", message="partial outage"),
        )
        env["register_adapters"]({"cloud-a": adapter_a})

        alert_monitor.run_check(env["config"], env["state"])

        keys = set(env["state"]["last_alerts"].keys())
        assert "platform:Ntfy Delivery Failed" in keys
        # Must NOT be source-prefixed — the ntfy-delivery concern is global.
        assert "platform:cloud-a:Ntfy Delivery Failed" not in keys


class _RaisingAdapter(FakeAdapter):
    """FakeAdapter variant that lets the test choose where to raise.

    ``fetch_error`` is raised from ``fetch_datastores`` on the first call;
    ``health_error`` is raised from ``check_health``. Both are optional.
    """

    def __init__(self, source_id: str, *, fetch_error=None, health_error=None):
        super().__init__(source_id)
        self._fetch_error = fetch_error
        self._health_error = health_error

    def check_health(self) -> HealthResult:
        if self._health_error is not None:
            raise self._health_error
        return super().check_health()

    def fetch_datastores(self) -> list[dict]:
        self.fetch_datastores_calls += 1
        if self._fetch_error is not None:
            raise self._fetch_error
        return []


class TestPhase7MultiSourceHardening:
    def test_source_connection_error_during_fetch_datastores_emits_alert_and_continues(
        self, monitor_env,
    ):
        """TLS/connection failure in one adapter must not stop the next source."""
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "pbs-a", "type": "pbs",
             "base_url": "https://pbs-a", "node": "pbs", "token_env": "T"},
            {"id": "cloud-b", "type": "remote_backups",
             "api_base": "https://b", "api_key_env": "API_KEY"},
        ]
        adapter_a = _RaisingAdapter(
            "pbs-a",
            fetch_error=SourceConnectionError("TLS fingerprint mismatch"),
        )
        adapter_b = FakeAdapter(
            "cloud-b",
            datastores=[{"id": "ds-b", "name": "B", "metrics": {}}],
        )
        env["register_adapters"]({"pbs-a": adapter_a, "cloud-b": adapter_b})

        alert_monitor.run_check(env["config"], env["state"])

        assert adapter_a.fetch_datastores_calls == 1
        assert adapter_b.fetch_datastores_calls == 1
        assert "ds-b" in env["state"]["datastores"]
        assert "platform:pbs-a:Monitoring API Error" in env["state"]["last_alerts"]

    def test_unexpected_exception_in_source_body_isolates_failure(self, monitor_env):
        """Unexpected (non-whitelisted) exception in one source is contained."""
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "cloud-a", "type": "remote_backups",
             "api_base": "https://a", "api_key_env": "API_KEY"},
            {"id": "cloud-b", "type": "remote_backups",
             "api_base": "https://b", "api_key_env": "API_KEY"},
        ]
        # KeyError is not in the inner except tuple → must be caught by the
        # outer per-source try/except.
        adapter_a = _RaisingAdapter(
            "cloud-a",
            health_error=KeyError("missing-field"),
        )
        adapter_b = FakeAdapter(
            "cloud-b",
            datastores=[{"id": "ds-b", "name": "B", "metrics": {}}],
        )
        env["register_adapters"]({"cloud-a": adapter_a, "cloud-b": adapter_b})

        alert_monitor.run_check(env["config"], env["state"])

        assert adapter_b.fetch_datastores_calls == 1
        assert "ds-b" in env["state"]["datastores"]
        assert "platform:cloud-a:Monitoring API Error" in env["state"]["last_alerts"]

    def test_platform_alert_carries_source_id_and_display_title_shows_suffix(
        self, monitor_env,
    ):
        """A platform alert from a non-default source gets a labelled ntfy title.

        Raw ``alert.title`` must stay generic ("API Unhealthy") so the
        ``ALERT_TITLE_TO_TYPE_KEY`` lookup keeps matching for all sources.
        """
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "lab-pbs", "type": "pbs",
             "base_url": "https://pbs", "node": "pbs", "token_env": "T"},
        ]
        adapter = FakeAdapter(
            "lab-pbs",
            health=HealthResult(status="degraded", message="partial outage"),
        )
        env["register_adapters"]({"lab-pbs": adapter})

        alert_monitor.run_check(env["config"], env["state"])

        unhealthy_calls = [c for c in env["ntfy_calls"]
                           if c["key"] == "platform:lab-pbs:API Unhealthy"]
        assert len(unhealthy_calls) == 1
        call = unhealthy_calls[0]
        assert call["title"] == "API Unhealthy"
        assert call["source_id"] == "lab-pbs"
        assert call["display_title"] == "API Unhealthy [lab-pbs]"

    def test_per_alert_override_still_applies_for_source_tagged_alert(
        self, monitor_env,
    ):
        """A per-alert ``ignore`` override must silence source-tagged alerts.

        Proves that ``alert.title`` stays source-neutral so the type-key
        lookup in ``_resolve_alert_priority`` keeps matching when a non-
        default source emits one of the standard platform alerts.
        """
        env = monitor_env
        env["config"]["sources"] = [
            {"id": "pbs-1", "type": "pbs",
             "base_url": "https://pbs", "node": "pbs", "token_env": "T"},
        ]
        env["config"]["notification_priorities"] = {
            "warning": 4,
            "critical": 5,
            "per_alert": {"api_unhealthy": "ignore"},
        }
        adapter = FakeAdapter(
            "pbs-1",
            health=HealthResult(status="degraded", message="partial outage"),
        )
        env["register_adapters"]({"pbs-1": adapter})

        alert_monitor.run_check(env["config"], env["state"])

        unhealthy_calls = [c for c in env["ntfy_calls"]
                           if c["key"] == "platform:pbs-1:API Unhealthy"]
        assert unhealthy_calls == []
        # Also no last_alerts cooldown entry — the alert is suppressed before
        # should_alert() is reached.
        assert "platform:pbs-1:API Unhealthy" not in env["state"]["last_alerts"]
