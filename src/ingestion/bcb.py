"""BCB SGS ingestion for the CDI daily rate series (SGS code 12)."""

from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
import requests

from ..config import BCB_SGS_URL, PROCESSED_DIR, ensure_dirs
from ._utils import purge_old_snapshots, snapshot_path

logger = logging.getLogger(__name__)

BCB_PROCESSED = PROCESSED_DIR / "bcb"


def _stem(start: pd.Timestamp, end: pd.Timestamp) -> str:
    """Filename stem encoding the requested date range."""
    return f"cdi_daily_{start:%Y%m%d}_{end:%Y%m%d}"


def fetch_cdi_daily(
    start: pd.Timestamp,
    end: pd.Timestamp,
    timeout: int = 30,
    force: bool = False,
) -> pd.Series:
    """Fetch CDI daily rates between ``start`` and ``end``, as a date-indexed Series of decimal values."""
    ensure_dirs()
    BCB_PROCESSED.mkdir(parents=True, exist_ok=True)
    stem = _stem(pd.Timestamp(start), pd.Timestamp(end))
    cache = snapshot_path(BCB_PROCESSED, stem, ".parquet")

    if not force and cache.exists():
        s = pd.read_parquet(cache)["cdi_daily"]
        s.index.name = "data"
        logger.info(
            "CDI: %d trading days loaded from cache (%s → %s)",
            len(s),
            s.index.min().date(),
            s.index.max().date(),
        )
        return s

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
    out = df.set_index("data")["valor"].sort_index().rename("cdi_daily")

    out.to_frame().to_parquet(cache)
    purge_old_snapshots(BCB_PROCESSED, stem, ".parquet", keep=cache)

    logger.info(
        "CDI: %d trading days fetched (%s → %s)",
        len(out),
        out.index.min().date(),
        out.index.max().date(),
    )
    return out
