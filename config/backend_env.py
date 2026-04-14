"""Load backend root `.env` into `os.environ` (does not override existing vars)."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

_BACKEND_ROOT = Path(__file__).resolve().parents[1]


def load_backend_env() -> None:
    load_dotenv(_BACKEND_ROOT / ".env", override=False)
