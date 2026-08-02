"""Evaluation helpers for trajectory classifiers."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, f1_score


def classification_report_dict(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> dict[str, Any]:
    """Return sklearn classification metrics as a JSON-serializable dict."""
    report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "weighted_f1": float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
        "per_class": report,
    }
