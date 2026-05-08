"""ANBIMA public xlsx ingestion for RCVM 175 fund characteristics."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..config import RAW_DIR

ANBIMA_RAW = RAW_DIR / "anbima"
CARACTERISTICAS_FILE = ANBIMA_RAW / "FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx"


def fetch_caracteristicas(path: Path = CARACTERISTICAS_FILE) -> pd.DataFrame:
    """Read the ANBIMA characteristics xlsx and return the raw DataFrame."""
    return pd.read_excel(path)
