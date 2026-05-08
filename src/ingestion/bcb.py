"""BCB SGS ingestion for the CDI daily rate series (SGS code 12)."""

from __future__ import annotations

import logging
from datetime import date
from io import StringIO

import pandas as pd
import requests

from ..config import BCB_SGS_URL

logger = logging.getLogger(__name__)


def fetch_cdi_daily(
    start: date,
    end: date,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch CDI daily rates from BCB SGS between start and end; returns DataFrame with date and rate columns."""
    url = (
        f"{BCB_SGS_URL}?formato=json"
        f"&dataInicial={pd.Timestamp(start):%d/%m/%Y}"
        f"&dataFinal={pd.Timestamp(end):%d/%m/%Y}"
    )
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    df = pd.read_json(StringIO(r.text))
    df["data"] = pd.to_datetime(df["data"], dayfirst=True)
    df["valor"] = df["valor"].astype(float) / 100.0
    return df.rename(columns={"data": "date", "valor": "rate"}).sort_values("date")
