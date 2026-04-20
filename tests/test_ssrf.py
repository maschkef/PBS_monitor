"""Unit tests for the SSRF-protection validator ``_validate_ntfy_url``.

All tests call the function directly — no HTTP server required.

Scenarios covered
-----------------
Accepted (should not raise):
  - Public HTTPS URL with a resolvable hostname.
  - Public HTTP URL.

Rejected (must raise ``ValueError``):
  - Non-http/https scheme (ftp://, file://).
  - URL with no hostname.
  - Hostname resolving to 127.x loopback.
  - Hostname resolving to ::1 (IPv6 loopback).
  - Direct loopback IP literal (127.0.0.1, ::1).
  - Direct RFC-1918 private IP literals (10.x, 192.168.x, 172.16.x).
  - Cloud metadata address 169.254.169.254 (literal IP).
  - Link-local IPv6 (fe80::1).
  - Unresolvable hostname.
"""
import ipaddress
import socket
from unittest.mock import patch, MagicMock

import pytest

from webui.app import _validate_ntfy_url


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fake_getaddrinfo(address: str):
    """Return mock getaddrinfo tuples for a given IP address string."""
    ip = ipaddress.ip_address(address)
    family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
    return [(family, socket.SOCK_STREAM, 0, "", (address, 0))]


def _mock_resolve_as(ip_str: str):
    """Context manager: patch socket.getaddrinfo to resolve any host to *ip_str*."""
    return patch("webui.validators.socket.getaddrinfo", return_value=_fake_getaddrinfo(ip_str))


# ── Accepted URLs ─────────────────────────────────────────────────────────────

class TestValidNtfyUrls:
    def test_public_https_url(self):
        with _mock_resolve_as("1.2.3.4"):
            _validate_ntfy_url("https://ntfy.sh")  # must not raise

    def test_public_http_url(self):
        with _mock_resolve_as("5.6.7.8"):
            _validate_ntfy_url("http://ntfy.example.com/topic")  # must not raise

    def test_https_with_port(self):
        with _mock_resolve_as("203.0.113.1"):
            _validate_ntfy_url("https://example.com:8443/events")  # must not raise


# ── Rejected: scheme ─────────────────────────────────────────────────────────

class TestInvalidScheme:
    def test_ftp_scheme_rejected(self):
        with pytest.raises(ValueError, match="http"):
            _validate_ntfy_url("ftp://example.com/topic")

    def test_file_scheme_rejected(self):
        with pytest.raises(ValueError):
            _validate_ntfy_url("file:///etc/passwd")

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            _validate_ntfy_url("")


# ── Rejected: hostname issues ─────────────────────────────────────────────────

class TestInvalidHostname:
    def test_missing_hostname_rejected(self):
        with pytest.raises(ValueError, match="hostname"):
            _validate_ntfy_url("https:///path")

    def test_unresolvable_hostname_rejected(self):
        with patch("webui.validators.socket.getaddrinfo", side_effect=OSError("Name not found")):
            with pytest.raises(ValueError, match="resolved"):
                _validate_ntfy_url("https://does-not-exist-xyz.invalid/topic")


# ── Rejected: private / reserved addresses ────────────────────────────────────

class TestPrivateAddresses:
    @pytest.mark.parametrize("ip", [
        "127.0.0.1",
        "127.100.200.1",
    ])
    def test_loopback_ipv4_rejected(self, ip):
        with _mock_resolve_as(ip):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url(f"https://somehost.example.com/")

    def test_loopback_ipv6_rejected(self):
        with _mock_resolve_as("::1"):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://somehost.example.com/")

    @pytest.mark.parametrize("ip", [
        "10.0.0.1",
        "10.255.255.255",
        "192.168.1.1",
        "172.16.0.1",
        "172.31.255.254",
    ])
    def test_rfc1918_private_range_rejected(self, ip):
        with _mock_resolve_as(ip):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://internal.example.com/")

    def test_cloud_metadata_ip_rejected(self):
        with _mock_resolve_as("169.254.169.254"):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://metadata.internal/")

    def test_link_local_ipv4_rejected(self):
        with _mock_resolve_as("169.254.0.1"):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://somehost.example.com/")

    def test_ula_ipv6_rejected(self):
        with _mock_resolve_as("fc00::1"):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://somehost.example.com/")

    def test_link_local_ipv6_rejected(self):
        with _mock_resolve_as("fe80::1"):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url("https://somehost.example.com/")


# ── Rejected: literal IP in URL ───────────────────────────────────────────────

class TestLiteralIPInUrl:
    @pytest.mark.parametrize("url", [
        "https://127.0.0.1/topic",
        "https://192.168.1.100/topic",
        "https://10.0.0.1/topic",
        "https://169.254.169.254/latest/meta-data/",
    ])
    def test_private_literal_ip_in_url_rejected(self, url):
        # Extract IP from URL and make getaddrinfo return it directly.
        from urllib.parse import urlparse
        host = urlparse(url).hostname
        with _mock_resolve_as(host):
            with pytest.raises(ValueError, match="private or reserved"):
                _validate_ntfy_url(url)
