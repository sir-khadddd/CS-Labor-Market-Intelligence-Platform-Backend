"""Unit tests for load_postgres table resolution."""

import pytest

from config.postgres import ANALYTICS_TABLES
from scripts.load_postgres import _resolve_tables


def test_resolve_tables_default_returns_all():
    assert _resolve_tables(None) == list(ANALYTICS_TABLES)


def test_resolve_tables_valid_subset():
    selected = _resolve_tables("trajectory_features,trajectory_labels")
    assert selected == ["trajectory_features", "trajectory_labels"]


def test_resolve_tables_unknown_raises():
    with pytest.raises(ValueError, match="Unknown table"):
        _resolve_tables("trajectory_features,not_a_table")
