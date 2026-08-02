"""Unit tests for ML_MODEL_PATH containment in the trajectory router."""

from pathlib import Path

from api.routers.trajectory import _get_model_path, _load_cached_model
from ml.constants import DEFAULT_ARTIFACTS_DIR, DEFAULT_MODEL_FILENAME


def test_default_model_path_used_without_override(monkeypatch):
    monkeypatch.delenv("ML_MODEL_PATH", raising=False)
    assert _get_model_path() == DEFAULT_ARTIFACTS_DIR / DEFAULT_MODEL_FILENAME


def test_override_inside_artifacts_dir_is_accepted(monkeypatch):
    override = DEFAULT_ARTIFACTS_DIR / "candidate" / "model.joblib"
    monkeypatch.setenv("ML_MODEL_PATH", str(override))
    assert _get_model_path() == override.resolve()


def test_override_outside_artifacts_dir_is_rejected(monkeypatch, tmp_path: Path, caplog):
    outside = tmp_path / "evil.joblib"
    outside.write_bytes(b"not a model")
    monkeypatch.setenv("ML_MODEL_PATH", str(outside))
    with caplog.at_level("WARNING"):
        assert _get_model_path() is None
    assert any("outside" in record.message for record in caplog.records)


def test_traversal_out_of_artifacts_dir_is_rejected(monkeypatch):
    monkeypatch.setenv(
        "ML_MODEL_PATH", str(DEFAULT_ARTIFACTS_DIR / ".." / ".." / "model.joblib")
    )
    assert _get_model_path() is None


def test_rejected_override_yields_no_model(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("ML_MODEL_PATH", str(tmp_path / "evil.joblib"))
    assert _load_cached_model() is None
