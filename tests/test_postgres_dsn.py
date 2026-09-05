"""Tests for Postgres DSN resolution from environment variables."""

import os

import pytest

from config.postgres import DEFAULT_POSTGRES_DSN, get_postgres_dsn


@pytest.fixture(autouse=True)
def _clear_dsn_env(monkeypatch):
    monkeypatch.delenv("CS_LMI_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)


def test_get_postgres_dsn_default_when_unset():
    assert get_postgres_dsn() == DEFAULT_POSTGRES_DSN


def test_get_postgres_dsn_prefers_cs_lmi(monkeypatch):
    monkeypatch.setenv("CS_LMI_POSTGRES_DSN", "postgresql://cs:secret@db:5432/cs_lmi")
    monkeypatch.setenv("DATABASE_URL", "postgresql://other:secret@db:5432/other")
    assert get_postgres_dsn() == "postgresql://cs:secret@db:5432/cs_lmi"


def test_get_postgres_dsn_uses_database_url_alias(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://alias:secret@db:5432/app")
    assert get_postgres_dsn() == "postgresql://alias:secret@db:5432/app"
