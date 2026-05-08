"""CVM Dados Abertos ingestion: fund registry, daily quotes, and fee history."""

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


def fmt_cnpj(value: str | int) -> str:
    """Format any CNPJ value (raw digits, integer, or already-formatted) to XX.XXX.XXX/XXXX-XX."""
    d = "".join(c for c in str(value) if c.isdigit()).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _ensure_cvm_dirs() -> None:
    ensure_dirs()
    CVM_RAW.mkdir(parents=True, exist_ok=True)
    CVM_PROCESSED.mkdir(parents=True, exist_ok=True)


REGISTRO_TABLES = ("registro_fundo", "registro_classe", "registro_subclasse")


def fetch_registro_fundo_classe(force: bool = False) -> dict[str, pd.DataFrame]:
    """Download registro_fundo_classe.zip and return its three CSVs keyed by table name."""
    _ensure_cvm_dirs()
    today = today_stamp()
    parquets = {
        n: snapshot_path(CVM_PROCESSED, n, ".parquet", today) for n in REGISTRO_TABLES
    }

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
    """Download monthly inf_diario_fi files for the date range and return a concatenated DataFrame.

    ``universe_keys`` takes precedence over ``universe_cnpjs`` when both are given;
    use it when you also need to filter by ID_SUBCLASSE.
    """
    _ensure_cvm_dirs()
    months = _yyyymm_range(start, end)
    frames: list[pd.DataFrame] = []

    _keys_cnpjs: set[str] | None = None
    _keys_set: set[tuple[str, str]] | None = None  # NaN subclasse → ""
    if universe_keys is not None:
        _keys_cnpjs = {c for c, _ in universe_keys}
        _keys_set = {(c, s if s is not None else "") for c, s in universe_keys}

    current_ym = date.today().strftime("%Y%m")
    keep_cols = [
        "TP_FUNDO_CLASSE",
        "CNPJ_FUNDO_CLASSE",
        "ID_SUBCLASSE",
        "DT_COMPTC",
        "VL_QUOTA",
        "VL_PATRIM_LIQ",
        "CAPTC_DIA",
        "RESG_DIA",
        "NR_COTST",
    ]

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

            # Post-RCVM175 files use CNPJ_FUNDO_CLASSE; older files use CNPJ_FUNDO.
            cnpj_col = next((c for c in df.columns if "CNPJ_FUNDO" in c), None)
            if cnpj_col and cnpj_col != "CNPJ_FUNDO_CLASSE":
                df = df.rename(columns={cnpj_col: "CNPJ_FUNDO_CLASSE"})

            df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
            for col in (
                "VL_QUOTA",
                "VL_PATRIM_LIQ",
                "VL_TOTAL",
                "CAPTC_DIA",
                "RESG_DIA",
            ):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "NR_COTST" in df.columns:
                df["NR_COTST"] = pd.to_numeric(df["NR_COTST"], errors="coerce").astype(
                    "Int32"
                )

            df = df[[c for c in keep_cols if c in df.columns]]
            cache.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache, index=False)
            if ym == current_ym:
                purge_old_snapshots(CVM_PROCESSED, stem, ".parquet", keep=cache)

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
    logger.debug(
        "inf_diario: %d rows loaded for %d funds",
        len(out),
        out["CNPJ_FUNDO_CLASSE"].nunique(),
    )
    return out


_INF_GROUP_KEY = ["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE"]
_INF_ROW_KEY = _INF_GROUP_KEY + ["DT_COMPTC"]


def clean_inf_diario(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate, drop ghost rows, mask bad zeros, and forward-fill VL_QUOTA."""
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
            logger.debug(
                "inf_diario: dropped %d dead rows (quota=0, cotistas=0)", n_dead
            )
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
    """Fetch and clean inf_diario for the date window; prefer this over ``fetch_inf_diario`` directly."""
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


def fetch_extrato(year: int, force: bool = False) -> pd.DataFrame:
    """Download the annual extrato_fi CSV for the given year."""
    _ensure_cvm_dirs()
    url = f"{CVM_BASE_URL}/DOC/EXTRATO/DADOS/extrato_fi_{year}.csv"

    if year == date.today().year:
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
    """Download cad_fi_hist.zip and return its CSVs as ``{basename: DataFrame}``.

    Pass ``members`` to read only specific CSVs (e.g. ``["cad_fi_hist_taxa_adm"]``).
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
    """Return the most-recent administration and performance fees per fund from cad_fi_hist.

    Output columns: CNPJ_FUNDO_CLASSE, adm_fee, adm_fee_dt, perf_fee, perf_fee_desc,
    perf_fee_dt, has_perf_fee.
    """
    cache = snapshot_path(CVM_PROCESSED, "cad_fi_taxa", ".parquet")
    if not force and cache.exists():
        return pd.read_parquet(cache)

    members = ["cad_fi_hist_taxa_adm", "cad_fi_hist_taxa_perfm"]
    tables = fetch_cad_fi_hist(members=members, force=force)

    adm_raw = tables["cad_fi_hist_taxa_adm"].copy()
    adm_raw["CNPJ_FUNDO_CLASSE"] = adm_raw["CNPJ_FUNDO"].map(fmt_cnpj)
    adm_raw["DT_INI_TAXA_ADM"] = pd.to_datetime(
        adm_raw["DT_INI_TAXA_ADM"], errors="coerce"
    )
    adm_raw["TAXA_ADM"] = pd.to_numeric(adm_raw["TAXA_ADM"], errors="coerce")
    adm = (
        adm_raw.sort_values("DT_INI_TAXA_ADM")
        .groupby("CNPJ_FUNDO_CLASSE", as_index=False)
        .last()[["CNPJ_FUNDO_CLASSE", "TAXA_ADM", "DT_INI_TAXA_ADM"]]
        .rename(columns={"TAXA_ADM": "adm_fee", "DT_INI_TAXA_ADM": "adm_fee_dt"})
    )

    perf_raw = tables["cad_fi_hist_taxa_perfm"].copy()
    perf_raw["CNPJ_FUNDO_CLASSE"] = perf_raw["CNPJ_FUNDO"].map(fmt_cnpj)
    perf_raw["DT_INI_TAXA_PERFM"] = pd.to_datetime(
        perf_raw["DT_INI_TAXA_PERFM"], errors="coerce"
    )
    perf_raw["VL_TAXA_PERFM"] = pd.to_numeric(
        perf_raw["VL_TAXA_PERFM"], errors="coerce"
    )
    perf = (
        perf_raw.sort_values("DT_INI_TAXA_PERFM")
        .groupby("CNPJ_FUNDO_CLASSE", as_index=False)
        .last()[
            ["CNPJ_FUNDO_CLASSE", "VL_TAXA_PERFM", "DS_TAXA_PERFM", "DT_INI_TAXA_PERFM"]
        ]
        .rename(
            columns={
                "VL_TAXA_PERFM": "perf_fee",
                "DS_TAXA_PERFM": "perf_fee_desc",
                "DT_INI_TAXA_PERFM": "perf_fee_dt",
            }
        )
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
    """Return the most-recent adm_fee and has_perf_fee per fund from extrato_fi.

    Output columns: CNPJ_FUNDO_CLASSE (formatted), adm_fee (float), has_perf_fee (bool).
    """
    _ensure_cvm_dirs()
    stem = f"extrato_taxa_{year}"
    if year == date.today().year:
        cache = snapshot_path(CVM_PROCESSED, stem, ".parquet")
    else:
        cache = CVM_PROCESSED / f"{stem}.parquet"

    if not force and cache.exists():
        return pd.read_parquet(cache)

    df = fetch_extrato(year, force=force)
    df = df[["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "TAXA_ADM", "EXISTE_TAXA_PERFM"]].copy()
    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
    df["TAXA_ADM"] = pd.to_numeric(df["TAXA_ADM"], errors="coerce")

    dedup = (
        df.sort_values("DT_COMPTC")
        .groupby("CNPJ_FUNDO_CLASSE", as_index=False)
        .last()[["CNPJ_FUNDO_CLASSE", "TAXA_ADM", "EXISTE_TAXA_PERFM"]]
        .rename(columns={"TAXA_ADM": "adm_fee"})
    )
    dedup["has_perf_fee"] = dedup["EXISTE_TAXA_PERFM"].map({"S": True, "N": False})
    out = dedup[["CNPJ_FUNDO_CLASSE", "adm_fee", "has_perf_fee"]]

    cache.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cache, index=False)
    if year == date.today().year:
        purge_old_snapshots(CVM_PROCESSED, stem, ".parquet", keep=cache)
    return out
