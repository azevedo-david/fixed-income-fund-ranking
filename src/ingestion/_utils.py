"""Shared ingestion helpers: snapshot caching, HTTP download, zip/csv reading."""

from __future__ import annotations

import logging
import zipfile
from datetime import date as _date
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

CSV_READ_KWARGS = dict(sep=";", encoding="latin-1", low_memory=False)


def yyyymm_range(start: _date, end: _date) -> list[str]:
    """Inclusive list of YYYYMM strings between two dates."""
    out: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def today_stamp() -> str:
    """YYYYMMDD string used to version snapshot caches."""
    return _date.today().strftime("%Y%m%d")


def snapshot_path(
    directory: Path, stem: str, ext: str, today: str | None = None
) -> Path:
    """Path for today's snapshot of a dataset (e.g. ``cad_fi_taxa_20260501.parquet``)."""
    return directory / f"{stem}_{today or today_stamp()}{ext}"


def download(url: str, dest: Path, force: bool = False, timeout: int = 120) -> Path:
    """Stream-download ``url`` to ``dest`` with a progress bar. Cached on disk."""
    if dest.exists() and not force:
        logger.debug("cache hit: %s", dest.name)
        return dest

    logger.info("fetching %s", dest.name)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        with (
            open(tmp, "wb") as f,
            tqdm(
                total=total, unit="B", unit_scale=True, desc=dest.name, leave=False
            ) as bar,
        ):
            for chunk in r.iter_content(chunk_size=1 << 15):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
        tmp.replace(dest)
    return dest


def read_csv_or_zip(path: Path, member: str | None = None) -> pd.DataFrame:
    """Read a CSV directly, or open a (the first, by default) CSV member from a ZIP."""
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not members:
                raise FileNotFoundError(f"no csv inside {path}")
            target = member or members[0]
            with zf.open(target) as fh:
                return pd.read_csv(fh, **CSV_READ_KWARGS)
    return pd.read_csv(path, **CSV_READ_KWARGS)


def read_all_csvs_from_zip(path: Path) -> dict[str, pd.DataFrame]:
    """Return ``{member_name: DataFrame}`` for every CSV in a ZIP archive."""
    out: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as fh:
                out[name] = pd.read_csv(fh, **CSV_READ_KWARGS)
    return out
