"""Per-source ingest functions for raw.* tables; each maps to one task."""

from __future__ import annotations

import logging
from datetime import date

from tqdm import tqdm

from ..storage import DuckDBWarehouse
from .anbima_xlsx import fetch_caracteristicas
from .bcb import fetch_cdi_daily
from ._utils import yyyymm_range
from .cvm import (
    fetch_cad_fi_hist,
    fetch_extrato,
    fetch_inf_diario_month,
    fetch_registro_fundo_classe,
)

logger = logging.getLogger(__name__)

_HISTORY_START = date(2021, 1, 1)


def ingest_raw(
    db: DuckDBWarehouse,
    reference_date: date,
    force: bool = False,
) -> None:
    """CLI convenience wrapper — runs all sources in dependency order."""
    ingest_registro(db, force)
    ingest_inf_diario(db, reference_date, force)
    ingest_cad_fi_hist(db, force)
    ingest_extrato(db, reference_date, force)
    ingest_cdi(db, reference_date, force)
    ingest_anbima(db, force)


def _snapshot_loaded_today(db: DuckDBWarehouse, schema: str, table: str) -> bool:
    """Return True if today's snapshot is already present in schema.table."""
    exists = db.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    if not exists:
        return False
    row = db.execute(f"SELECT MAX(downloaded_at) FROM {schema}.{table}").fetchone()
    return bool(row and row[0] == date.today())


def ingest_registro(db: DuckDBWarehouse, force: bool = False) -> None:
    """Fetch CVM fund/class registry and append today's snapshot to raw.registro_*."""
    today = date.today()
    if not force and _snapshot_loaded_today(db, "raw", "registro_classe"):
        logger.info("ingest registro: today's snapshot already loaded, skipping")
        return
    tables = fetch_registro_fundo_classe(force=force)
    for table_key in ("registro_classe", "registro_subclasse"):
        if table_key in tables:
            db.append_snapshot("raw", table_key, tables[table_key], downloaded_at=today)
    logger.info("ingest registro: done")


def ingest_inf_diario(
    db: DuckDBWarehouse, reference_date: date, force: bool = False
) -> None:
    """Incrementally upsert monthly CVM daily-quote files into raw.inf_diario."""
    max_dt = db.get_max_date("raw", "inf_diario", "DT_COMPTC")
    start = max_dt if max_dt else _HISTORY_START
    months = yyyymm_range(start, reference_date)
    for ym in tqdm(months, desc="inf_diario", unit="month"):
        df = fetch_inf_diario_month(ym, force=force)
        if not df.empty:
            db.upsert_timeseries(
                "raw",
                "inf_diario",
                df,
                natural_key=["CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC"],
            )
    logger.info("ingest inf_diario: %d months", len(months))


def ingest_cad_fi_hist(db: DuckDBWarehouse, force: bool = False) -> None:
    """Fetch CVM historical fee tables and append today's snapshot to raw.cad_fi_hist_*."""
    today = date.today()
    if not force and _snapshot_loaded_today(db, "raw", "cad_fi_hist_taxa_adm"):
        logger.info("ingest cad_fi_hist: today's snapshot already loaded, skipping")
        return
    members = ["cad_fi_hist_taxa_adm", "cad_fi_hist_taxa_perfm"]
    tables = fetch_cad_fi_hist(members=members, force=force)
    for name, df in tables.items():
        db.append_snapshot("raw", name, df, downloaded_at=today)
    logger.info("ingest cad_fi_hist: done")


def ingest_extrato(
    db: DuckDBWarehouse, reference_date: date, force: bool = False
) -> None:
    """Fetch CVM extrato_fi for reference_date's year and append today's snapshot."""
    today = date.today()
    if not force and _snapshot_loaded_today(db, "raw", "extrato_fi"):
        logger.info("ingest extrato: today's snapshot already loaded, skipping")
        return
    df = fetch_extrato(reference_date.year, force=force)
    df = df[["CNPJ_FUNDO_CLASSE", "DT_COMPTC", "TAXA_ADM", "EXISTE_TAXA_PERFM"]]
    db.append_snapshot("raw", "extrato_fi", df, downloaded_at=today)
    logger.info("ingest extrato: done")


def ingest_cdi(db: DuckDBWarehouse, reference_date: date, force: bool = False) -> None:
    """Incrementally upsert BCB daily CDI rates into raw.cdi_daily."""
    max_dt = db.get_max_date("raw", "cdi_daily", "date")
    start = _HISTORY_START if force else (max_dt if max_dt else _HISTORY_START)
    df = fetch_cdi_daily(start=start, end=reference_date)
    if not df.empty:
        db.upsert_timeseries("raw", "cdi_daily", df, natural_key=["date"])
    logger.info("ingest cdi_daily: %d rows", len(df))


def ingest_anbima(db: DuckDBWarehouse, force: bool = False) -> None:
    """Fetch ANBIMA characteristics Excel and append today's snapshot to raw.anbima_caracteristicas."""
    today = date.today()
    if not force and _snapshot_loaded_today(db, "raw", "anbima_caracteristicas"):
        logger.info("ingest anbima: today's snapshot already loaded, skipping")
        return
    df = fetch_caracteristicas()
    db.append_snapshot("raw", "anbima_caracteristicas", df, downloaded_at=today)
    logger.info("ingest anbima: done")
