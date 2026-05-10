"""ANBIMA public xlsx ingestion for RCVM 175 fund characteristics."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ..config import RAW_DIR

ANBIMA_RAW = RAW_DIR / "anbima"
CARACTERISTICAS_FILE = ANBIMA_RAW / "FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx"

_MAX_FILE_AGE_DAYS = 7


def fetch_caracteristicas(path: Path = CARACTERISTICAS_FILE) -> pd.DataFrame:
    """Read the ANBIMA characteristics xlsx and return the raw DataFrame."""
    _guard_xlsx_age(path)
    return pd.read_excel(path)


def _guard_xlsx_age(path: Path) -> None:
    """Raise if the xlsx file does not exist or is older than _MAX_FILE_AGE_DAYS."""
    if not path.exists():
        raise FileNotFoundError(
            f"ANBIMA xlsx not found: {path}. Download it manually from the ANBIMA portal."
        )
    age_days = (date.today() - date.fromtimestamp(path.stat().st_mtime)).days
    if age_days > _MAX_FILE_AGE_DAYS:
        raise RuntimeError(
            f"ANBIMA xlsx is {age_days} days old (max {_MAX_FILE_AGE_DAYS}): {path}. "
            "Download a fresh copy from the ANBIMA portal."
        )
