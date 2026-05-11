"""CVM Dados Abertos ingestion: fund registry, daily quotes, and fee history."""

from __future__ import annotations

import logging
import zipfile
from datetime import date

import pandas as pd
import requests

from ..config import CVM_BASE_URL, RAW_DIR, ensure_dirs
from ._utils import (
    CSV_READ_KWARGS,
    download,
    read_csv_or_zip,
    snapshot_path,
    today_stamp,
)

logger = logging.getLogger(__name__)

CVM_RAW = RAW_DIR / "cvm"

REGISTRO_TABLES = ("registro_fundo", "registro_classe", "registro_subclasse")

_INF_KEEP_COLS = [
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


def _ensure_cvm_dirs() -> None:
    ensure_dirs()
    CVM_RAW.mkdir(parents=True, exist_ok=True)


def fetch_registro_fundo_classe(force: bool = False) -> dict[str, pd.DataFrame]:
    """Download registro_fundo_classe.zip and return its three CSVs keyed by table name."""
    _ensure_cvm_dirs()
    today = today_stamp()
    zip_path = snapshot_path(CVM_RAW, "registro_fundo_classe", ".zip", today)
    url = f"{CVM_BASE_URL}/CAD/DADOS/registro_fundo_classe.zip"
    download(url, zip_path, force=force)

    tables: dict[str, pd.DataFrame] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            dtype_override = {"CNPJ_Classe": str, "CNPJ_Fundo": str}
            with zf.open(name) as fh:
                key = name.replace(".csv", "")
                tables[key] = pd.read_csv(fh, **CSV_READ_KWARGS, dtype=dtype_override)
    return tables


def fetch_inf_diario_month(ym: str, force: bool = False) -> pd.DataFrame:
    """Download one month of inf_diario_fi and return a typed DataFrame.

    Renames CNPJ_FUNDO → CNPJ_FUNDO_CLASSE for pre-RCVM175 files (schema normalisation, not a transform).
    """
    _ensure_cvm_dirs()
    url = f"{CVM_BASE_URL}/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ym}.zip"
    stem = f"inf_diario_fi_{ym}"
    current_ym = date.today().strftime("%Y%m")
    if ym == current_ym:
        # CVM updates the current month's file daily — use a dated filename
        # so yesterday's cached copy is not reused.
        path = snapshot_path(CVM_RAW, stem, ".zip")
    else:
        path = CVM_RAW / f"{stem}.zip"

    try:
        download(url, path, force=force)
    except requests.HTTPError as e:
        logger.warning("skipping %s: %s", ym, e)
        return pd.DataFrame()

    df = read_csv_or_zip(path)

    cnpj_col = next((c for c in df.columns if "CNPJ_FUNDO" in c), None)
    if cnpj_col and cnpj_col != "CNPJ_FUNDO_CLASSE":
        df = df.rename(columns={cnpj_col: "CNPJ_FUNDO_CLASSE"})
    if "TP_FUNDO" in df.columns and "TP_FUNDO_CLASSE" not in df.columns:
        df = df.rename(columns={"TP_FUNDO": "TP_FUNDO_CLASSE"})
    if "ID_SUBCLASSE" not in df.columns:
        df["ID_SUBCLASSE"] = None

    df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")
    for col in ("VL_QUOTA", "VL_PATRIM_LIQ", "VL_TOTAL", "CAPTC_DIA", "RESG_DIA"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "NR_COTST" in df.columns:
        df["NR_COTST"] = pd.to_numeric(df["NR_COTST"], errors="coerce").astype("Int32")

    return df[[c for c in _INF_KEEP_COLS if c in df.columns]]


def fetch_extrato(year: int, force: bool = False) -> pd.DataFrame:
    """Download the annual extrato_fi CSV for the given year and return the raw DataFrame."""
    _ensure_cvm_dirs()
    url = f"{CVM_BASE_URL}/DOC/EXTRATO/DADOS/extrato_fi_{year}.csv"

    if year == date.today().year:
        path = snapshot_path(CVM_RAW, f"extrato_fi_{year}", ".csv")
    else:
        path = CVM_RAW / f"extrato_fi_{year}.csv"

    download(url, path, force=force)
    return read_csv_or_zip(path)


def fetch_cad_fi_hist(
    members: list[str] | None = None,
    force: bool = False,
) -> dict[str, pd.DataFrame]:
    """Download cad_fi_hist.zip and return its CSVs as {basename: DataFrame}.

    Pass members to read only specific CSVs (e.g. ["cad_fi_hist_taxa_adm"]).
    """
    _ensure_cvm_dirs()
    zip_path = snapshot_path(CVM_RAW, "cad_fi_hist", ".zip")
    url = f"{CVM_BASE_URL}/CAD/DADOS/cad_fi_hist.zip"
    download(url, zip_path, force=force)

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
