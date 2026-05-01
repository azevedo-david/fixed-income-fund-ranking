"""CVM Dados Abertos ingestion.

Downloads bulk CSV/ZIP files from https://dados.cvm.gov.br/ and parses them
into pandas DataFrames. Files are cached locally under data/raw/cvm/.

For local testing we save parsed DataFrames as Parquet under data/processed/cvm/.

Universe definition:
    Only funds that have migrated to RCVM 175 are considered. The source of
    truth is registro_fundo_classe.zip (registro_fundo.csv +
    registro_classe.csv). Funds without Data_Adaptacao_RCVM175 are excluded.
"""
from __future__ import annotations

import logging
import zipfile
from datetime import date

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from ..config import CVM_BASE_URL, PROCESSED_DIR, RAW_DIR, ensure_dirs
from ._utils import (
    CSV_READ_KWARGS,
    download,
    purge_old_snapshots,
    read_csv_or_zip,
    snapshot_path,
    today_stamp,
)

logger = logging.getLogger(__name__)

CVM_RAW = RAW_DIR / "cvm"
CVM_PROCESSED = PROCESSED_DIR / "cvm"


def fmt_cnpj(value: str) -> str:
    """Format a raw CNPJ string to XX.XXX.XXX/XXXX-XX.

    Expects value to already be a string (enforce via dtype={'CNPJ_Classe': str}
    on read). Leading zeros missing from the raw data are restored by zfill(14).
    """
    d = value.strip().zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _ensure_cvm_dirs() -> None:
    ensure_dirs()
    CVM_RAW.mkdir(parents=True, exist_ok=True)
    CVM_PROCESSED.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Fund registry (RCVM 175)
# ---------------------------------------------------------------------------

REGISTRO_TABLES = ("registro_fundo", "registro_classe", "registro_subclasse")


def fetch_registro_fundo_classe(force: bool = False) -> dict[str, pd.DataFrame]:
    """Download registro_fundo_classe.zip and return its 3 CSVs.

    Returns a dict keyed by 'registro_fundo', 'registro_classe',
    'registro_subclasse'. CNPJ_Classe is read as str to preserve leading zeros.

    The registry is a single always-latest snapshot, so it is cached with the
    download date in the filename (e.g. ``registro_fundo_20260501.parquet``).
    A second call on the same day uses the parquet directly; a call on the
    next day re-downloads and replaces the previous snapshot.
    """
    _ensure_cvm_dirs()
    today = today_stamp()
    parquets = {n: snapshot_path(CVM_PROCESSED, n, ".parquet", today) for n in REGISTRO_TABLES}

    if not force and all(p.exists() for p in parquets.values()):
        return {n: pd.read_parquet(p) for n, p in parquets.items()}

    zip_path = snapshot_path(CVM_RAW, "registro_fundo_classe", ".zip", today)
    url = f"{CVM_BASE_URL}/CAD/DADOS/registro_fundo_classe.zip"
    download(url, zip_path, force=force)
    purge_old_snapshots(CVM_RAW, "registro_fundo_classe", ".zip", keep=zip_path)

    tables: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            dtype_override = {"CNPJ_Classe": str, "CNPJ_Fundo": str}
            with zf.open(name) as fh:
                key = name.replace(".csv", "")
                tables[key] = pd.read_csv(fh, **CSV_READ_KWARGS, dtype=dtype_override)

    for n, df in tables.items():
        if n in parquets:
            df.to_parquet(parquets[n], index=False)
            purge_old_snapshots(CVM_PROCESSED, n, ".parquet", keep=parquets[n])
    return tables

# ---------------------------------------------------------------------------
# Daily quota / PL
# ---------------------------------------------------------------------------

def _yyyymm_range(start: date, end: date) -> list[str]:
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


def fetch_inf_diario(
    start: date,
    end: date,
    universe_cnpjs: set[str] | None = None,
    universe_keys: set[tuple[str, str | None]] | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Download monthly inf_diario_fi files for the given date range.

    Args:
        start: First month to download (inclusive).
        end: Last month to download (inclusive).
        universe_cnpjs: If provided, filter to these CNPJ_FUNDO_CLASSE values.
            Kept for backwards compatibility; prefer universe_keys when you also
            need to restrict by ID_SUBCLASSE.
        universe_keys: If provided, filter to these (CNPJ_FUNDO_CLASSE,
            ID_SUBCLASSE) pairs. ID_SUBCLASSE=None matches rows where
            ID_SUBCLASSE is NaN. Takes precedence over universe_cnpjs when
            both are given.
        force: Re-download even if the ZIP is already cached.

    Returns a single concatenated DataFrame with typed columns:
        TP_FUNDO_CLASSE (str), CNPJ_FUNDO_CLASSE (str), ID_SUBCLASSE (str),
        DT_COMPTC (date), VL_QUOTA (float64), VL_PATRIM_LIQ (float64),
        CAPTC_DIA (float64), RESG_DIA (float64), NR_COTST (Int32).
    """
    _ensure_cvm_dirs()
    months = _yyyymm_range(start, end)
    frames: list[pd.DataFrame] = []

    # Pre-compute lookup structures once, outside the per-month loop.
    _keys_cnpjs: set[str] | None = None
    _keys_set: set[tuple[str, str]] | None = None  # NaN subclasse → ""
    if universe_keys is not None:
        _keys_cnpjs = {c for c, _ in universe_keys}
        _keys_set = {(c, s if s is not None else "") for c, s in universe_keys}

    from datetime import date as _date
    current_ym = _date.today().strftime("%Y%m")
    keep_cols = ["TP_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE",
                 "DT_COMPTC", "VL_QUOTA", "VL_PATRIM_LIQ",
                 "CAPTC_DIA", "RESG_DIA", "NR_COTST"]

    for ym in tqdm(months, desc="inf_diario", unit="month"):
        stem = f"inf_diario_fi_{ym}"
        if ym == current_ym:
            cache = snapshot_path(CVM_PROCESSED, stem, ".parquet")
        else:
            cache = CVM_PROCESSED / f"{stem}.parquet"

        if not force and cache.exists():
            df = pd.read_parquet(cache)
        else:
            url = f"{CVM_BASE_URL}/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ym}.zip"
            path = CVM_RAW / f"inf_diario_fi_{ym}.zip"
            try:
                download(url, path, force=force)
            except requests.HTTPError as e:
                logger.warning("skipping %s: %s", ym, e)
                continue

            df = read_csv_or_zip(path)

            # Normalise CNPJ column name — post-RCVM175 files use CNPJ_FUNDO_CLASSE,
            # older files may use CNPJ_FUNDO. Rename to a single consistent key.
            cnpj_col = next((c for c in df.columns if "CNPJ_FUNDO" in c), None)
            if cnpj_col and cnpj_col != "CNPJ_FUNDO_CLASSE":
                df = df.rename(columns={cnpj_col: "CNPJ_FUNDO_CLASSE"})

            # Cast to stable dtypes before saving to parquet.
            df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
            for col in ("VL_QUOTA", "VL_PATRIM_LIQ", "VL_TOTAL", "CAPTC_DIA", "RESG_DIA"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "NR_COTST" in df.columns:
                df["NR_COTST"] = pd.to_numeric(df["NR_COTST"], errors="coerce").astype("Int32")

            df = df[[c for c in keep_cols if c in df.columns]]
            cache.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache, index=False)
            if ym == current_ym:
                purge_old_snapshots(CVM_PROCESSED, stem, ".parquet", keep=cache)

        # Filter to universe (applied to cached or freshly-typed df).
        if universe_keys is not None:
            df = df[df["CNPJ_FUNDO_CLASSE"].isin(_keys_cnpjs)]
            if not df.empty and "ID_SUBCLASSE" in df.columns:
                sub_filled = df["ID_SUBCLASSE"].fillna("")
                mask = pd.MultiIndex.from_arrays(
                    [df["CNPJ_FUNDO_CLASSE"], sub_filled]
                ).isin(_keys_set)
                df = df[mask]
        elif universe_cnpjs:
            df = df[df["CNPJ_FUNDO_CLASSE"].isin(universe_cnpjs)]

        if df.empty:
            continue

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True).sort_values(
        ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"]
    )
    logger.debug("inf_diario: %d rows loaded for %d funds", len(out), out["CNPJ_FUNDO_CLASSE"].nunique())
    return out


# ---------------------------------------------------------------------------
# inf_diario cleaning + canonical loader
# ---------------------------------------------------------------------------

_INF_GROUP_KEY = ["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"]
_INF_ROW_KEY = _INF_GROUP_KEY + ["DT_COMPTC"]


def clean_inf_diario(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standard cleaning to a typed inf_diario DataFrame.

    Steps (in order):
      1. Deduplicate (CNPJ, ID_SUBCLASSE, DT_COMPTC). The same fund can appear
         twice on the same day when TP_FUNDO_CLASSE changes (administrative
         reclassification); keep the alphabetically-first TP_FUNDO_CLASSE.
      2. Drop rows where VL_QUOTA == 0 AND NR_COTST == 0 (liquidation ghosts
         with no economic content).
      3. Mask VL_QUOTA == 0 → NaN where the fund is still active (cotistas > 0
         or PL > 0); the previous valid quota will be carried forward.
      4. Forward-fill VL_QUOTA within (CNPJ, ID_SUBCLASSE) to cover NaN gaps.
         Leading NaNs (before the first valid quota) remain NaN.
    """
    out = df.copy()

    n_before = len(out)
    out = (
        out.sort_values(_INF_ROW_KEY + ["TP_FUNDO_CLASSE"], na_position="first")
           .drop_duplicates(subset=_INF_ROW_KEY, keep="first")
           .sort_values(_INF_ROW_KEY)
           .reset_index(drop=True)
    )
    n_dups = n_before - len(out)
    if n_dups:
        logger.debug("inf_diario: removed %d duplicate rows", n_dups)

    if "NR_COTST" in out.columns:
        mask_dead = (out["VL_QUOTA"] == 0) & (out["NR_COTST"] == 0)
        n_dead = int(mask_dead.sum())
        if n_dead:
            logger.debug("inf_diario: dropped %d dead rows (quota=0, cotistas=0)", n_dead)
        out = out[~mask_dead].reset_index(drop=True)

    mask_zero = out["VL_QUOTA"] == 0
    if "NR_COTST" in out.columns and "VL_PATRIM_LIQ" in out.columns:
        active = (out["NR_COTST"].fillna(0) > 0) | (out["VL_PATRIM_LIQ"].fillna(0) > 0)
        mask_bad_zero = mask_zero & active
    else:
        mask_bad_zero = mask_zero

    n_bad_zero = int(mask_bad_zero.sum())
    if n_bad_zero:
        logger.debug("inf_diario: masked %d zero-quota rows (active fund)", n_bad_zero)
        out.loc[mask_bad_zero, "VL_QUOTA"] = np.nan

    out["VL_QUOTA"] = out.groupby(_INF_GROUP_KEY, dropna=False)["VL_QUOTA"].ffill()
    return out


def load_inf_diario(
    start: date,
    end: date,
    universe_cnpjs: set[str] | None = None,
    universe_keys: set[tuple[str, str | None]] | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Fetch inf_diario for the window and apply standard cleaning.

    This is the canonical loader for any consumer that needs a clean
    inf_diario DataFrame. Callers needing the raw data should use
    ``fetch_inf_diario`` directly.
    """
    raw = fetch_inf_diario(
        start=start,
        end=end,
        universe_cnpjs=universe_cnpjs,
        universe_keys=universe_keys,
        force=force,
    )
    if raw.empty:
        return raw
    return clean_inf_diario(raw)


# ---------------------------------------------------------------------------
# Extrato (annual snapshot of detailed fees / limits)
# ---------------------------------------------------------------------------

def fetch_extrato(year: int, force: bool = False) -> pd.DataFrame:
    """Download the annual extrato_fi CSV for the given year.

    The current year's file is a snapshot that updates over time; older
    years are immutable. We use a date-versioned filename only for the
    current year so today's snapshot is reused within the day and refreshed
    on the next call after midnight.
    """
    from datetime import date as _date
    _ensure_cvm_dirs()
    url = f"{CVM_BASE_URL}/DOC/EXTRATO/DADOS/extrato_fi_{year}.csv"

    if year == _date.today().year:
        stem = f"extrato_fi_{year}"
        path = snapshot_path(CVM_RAW, stem, ".csv")
        download(url, path, force=force)
        purge_old_snapshots(CVM_RAW, stem, ".csv", keep=path)
    else:
        path = download(url, CVM_RAW / f"extrato_fi_{year}.csv", force=force)
    return read_csv_or_zip(path)


def fetch_cad_fi_hist(
    members: list[str] | None = None,
    force: bool = False,
) -> dict[str, pd.DataFrame]:
    """Download cad_fi_hist.zip and return its CSVs as a dict.

    Source: ``https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi_hist.zip``.
    The zip contains one file per cadastral attribute change history
    (taxa_adm, taxa_perfm, classe, condom, custodiante, ...). It is a single
    always-latest snapshot, so we cache it with the download date and replace
    older copies on the next-day call.

    Args:
        members: Optional list of CSV basenames (without .csv) to read; useful
            since the zip is large and most consumers only need a couple of
            files. If None, every CSV is read.
        force: Re-download even if today's snapshot is already cached.

    Returns a dict ``{member_name_without_ext: DataFrame}``.
    """
    _ensure_cvm_dirs()
    zip_path = snapshot_path(CVM_RAW, "cad_fi_hist", ".zip")
    url = f"{CVM_BASE_URL}/CAD/DADOS/cad_fi_hist.zip"
    download(url, zip_path, force=force)
    purge_old_snapshots(CVM_RAW, "cad_fi_hist", ".zip", keep=zip_path)

    wanted = set(members) if members else None
    out: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            base = name.removesuffix(".csv")
            if wanted is not None and base not in wanted:
                continue
            with zf.open(name) as fh:
                out[base] = pd.read_csv(fh, **CSV_READ_KWARGS)
    return out


def load_cad_fi_taxa(force: bool = False) -> pd.DataFrame:
    """Return the most-recent administration and performance fees per fund.

    Reads ``cad_fi_hist_taxa_adm.csv`` and ``cad_fi_hist_taxa_perfm.csv`` from
    cad_fi_hist.zip and keeps the latest entry per fund.

    Cached as parquet. Pass ``force=True`` to re-download.

    Output columns:
        CNPJ_FUNDO_CLASSE  str   (formatted XX.XXX.XXX/XXXX-XX)
        adm_fee            float (annual %)
        adm_fee_dt         date  (last change)
        perf_fee           float (%)
        perf_fee_desc      str   (benchmark / description)
        perf_fee_dt        date  (last change)
        has_perf_fee       bool  (True if perf_fee > 0)
    """
    cache = snapshot_path(CVM_PROCESSED, "cad_fi_taxa", ".parquet")
    if not force and cache.exists():
        return pd.read_parquet(cache)

    members = ["cad_fi_hist_taxa_adm", "cad_fi_hist_taxa_perfm"]
    tables = fetch_cad_fi_hist(members=members, force=force)

    def _to_formatted_cnpj(value) -> str:
        digits = "".join(c for c in str(value) if c.isdigit())
        return fmt_cnpj(digits)

    adm_raw = tables["cad_fi_hist_taxa_adm"].copy()
    adm_raw["CNPJ_FUNDO_CLASSE"] = adm_raw["CNPJ_FUNDO"].map(_to_formatted_cnpj)
    adm_raw["DT_INI_TAXA_ADM"] = pd.to_datetime(adm_raw["DT_INI_TAXA_ADM"], errors="coerce")
    adm_raw["TAXA_ADM"] = pd.to_numeric(adm_raw["TAXA_ADM"], errors="coerce")
    adm = (
        adm_raw.sort_values("DT_INI_TAXA_ADM")
               .groupby("CNPJ_FUNDO_CLASSE", as_index=False).last()
               [["CNPJ_FUNDO_CLASSE", "TAXA_ADM", "DT_INI_TAXA_ADM"]]
               .rename(columns={"TAXA_ADM": "adm_fee", "DT_INI_TAXA_ADM": "adm_fee_dt"})
    )

    perf_raw = tables["cad_fi_hist_taxa_perfm"].copy()
    perf_raw["CNPJ_FUNDO_CLASSE"] = perf_raw["CNPJ_FUNDO"].map(_to_formatted_cnpj)
    perf_raw["DT_INI_TAXA_PERFM"] = pd.to_datetime(perf_raw["DT_INI_TAXA_PERFM"], errors="coerce")
    perf_raw["VL_TAXA_PERFM"] = pd.to_numeric(perf_raw["VL_TAXA_PERFM"], errors="coerce")
    perf = (
        perf_raw.sort_values("DT_INI_TAXA_PERFM")
                .groupby("CNPJ_FUNDO_CLASSE", as_index=False).last()
                [["CNPJ_FUNDO_CLASSE", "VL_TAXA_PERFM", "DS_TAXA_PERFM", "DT_INI_TAXA_PERFM"]]
                .rename(columns={
                    "VL_TAXA_PERFM":     "perf_fee",
                    "DS_TAXA_PERFM":     "perf_fee_desc",
                    "DT_INI_TAXA_PERFM": "perf_fee_dt",
                })
    )
    perf["has_perf_fee"] = perf["perf_fee"].map(
        lambda x: (x > 0) if pd.notna(x) else np.nan
    )

    out = adm.merge(perf, how="outer", on="CNPJ_FUNDO_CLASSE")
    cache.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cache, index=False)
    purge_old_snapshots(CVM_PROCESSED, "cad_fi_taxa", ".parquet", keep=cache)
    return out


def load_extrato_taxa(year: int, force: bool = False) -> pd.DataFrame:
    """Return a slim, typed, deduplicated view of extrato_fi for fee enrichment.

    Keeps only the most recent record per CNPJ_FUNDO_CLASSE (sorted by DT_COMPTC).
    Current-year files are snapshot-cached (date-versioned); past years are fixed.

    Output columns:
        CNPJ_FUNDO_CLASSE  str   (formatted)
        adm_fee            float
        has_perf_fee       bool  (mapped from EXISTE_TAXA_PERFM)
    """
    from datetime import date as _date
    _ensure_cvm_dirs()
    stem = f"extrato_taxa_{year}"
    if year == _date.today().year:
        cache = snapshot_path(CVM_PROCESSED, stem, ".parquet")
    else:
        cache = CVM_PROCESSED / f"{stem}.parquet"

    if not force and cache.exists():
        return pd.read_parquet(cache)

    df = fetch_extrato(year, force=force)
    df = df[["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "TAXA_ADM", "EXISTE_TAXA_PERFM"]].copy()
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
    df["TAXA_ADM"]  = pd.to_numeric(df["TAXA_ADM"], errors="coerce")

    dedup = (
        df.sort_values("DT_COMPTC")
          .groupby("CNPJ_FUNDO_CLASSE", as_index=False).last()
          [["CNPJ_FUNDO_CLASSE", "TAXA_ADM", "EXISTE_TAXA_PERFM"]]
          .rename(columns={"TAXA_ADM": "adm_fee"})
    )
    dedup["has_perf_fee"] = dedup["EXISTE_TAXA_PERFM"].map({"S": True, "N": False})
    out = dedup[["CNPJ_FUNDO_CLASSE", "adm_fee", "has_perf_fee"]]

    cache.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cache, index=False)
    if year == _date.today().year:
        purge_old_snapshots(CVM_PROCESSED, stem, ".parquet", keep=cache)
    return out


