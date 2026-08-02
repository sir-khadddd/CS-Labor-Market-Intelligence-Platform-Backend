"""Unit tests for ML prediction Postgres cleanup behavior."""

from unittest.mock import MagicMock, patch

import pandas as pd

from ml.constants import METHOD, METHOD_VERSION
from scripts.predict_trajectory_model import _upsert_postgres


def test_upsert_postgres_deletes_all_ml_rows_for_entity_type():
    predictions = pd.DataFrame(
        [
            {
                "entity_type": "role",
                "entity_id": "R1",
                "month": pd.Timestamp("2024-01-01"),
                "trajectory_class": "stable_growth",
                "trajectory_score": float("nan"),
                "confidence": 0.9,
                "method": METHOD,
                "label_version": "phase1-v3",
                "method_version": METHOD_VERSION,
            }
        ]
    )

    cursor = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor

    with patch("scripts.predict_trajectory_model.psycopg.connect", return_value=connection):
        assert _upsert_postgres(predictions, "role") is True

    delete_sql, delete_params = cursor.execute.call_args_list[0].args
    assert "WHERE method = %s AND entity_type = %s" in delete_sql
    assert delete_params == (METHOD, "role")
