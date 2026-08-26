"""Tests for CORS origin configuration without a live server."""

import pytest

from api.main import _cors_settings


@pytest.fixture(autouse=True)
def _clear_cors_env(monkeypatch):
    monkeypatch.delenv("CORS_ORIGINS", raising=False)


def test_cors_default_wildcard_without_credentials():
    origins, allow_credentials = _cors_settings()
    assert origins == ["*"]
    assert allow_credentials is False


def test_cors_explicit_origins_allow_credentials(monkeypatch):
    monkeypatch.setenv(
        "CORS_ORIGINS",
        "https://app.example.com, https://admin.example.com",
    )
    origins, allow_credentials = _cors_settings()
    assert origins == ["https://app.example.com", "https://admin.example.com"]
    assert allow_credentials is True


def test_cors_empty_string_treated_as_unset(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", "   ")
    origins, allow_credentials = _cors_settings()
    assert origins == ["*"]
    assert allow_credentials is False
