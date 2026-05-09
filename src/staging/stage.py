"""Per-source staging functions; each maps to one Airflow task.

Each function owns the skip/force decision and the DB write.
The actual fetch + clean logic lives in the per-table modules.
"""

from __future__ import annotations

import logging

from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)

_DAILY_QUOTES_KEY = ["fund_cnpj", "subclass_id", "date"]
_CDI_RATES_KEY = ["date"]


def stage_all(db: DuckDBWarehouse, force: bool = False) -> None:
    """Run all staging steps in dependency order."""
    stage_registry(db, force=force)
    stage_fees(db, force=force)
    stage_daily_quotes(db, force=force)
    stage_cdi_rates(db, force=force)
    stage_anbima(db, force=force)


def stage_registry(db: DuckDBWarehouse, force: bool = False) -> int:
    from .registry import fetch_raw_registry

    if not force and _snapshot_current(db, "raw.registro_classe", "staging.registry"):
        logger.info("stage_registry: already up-to-date, skipping")
        return 0
    df = fetch_raw_registry(db)
    if df is None or df.empty:
        logger.warning("stage_registry: no raw data available")
        return 0
    acquired_date = df["reference_date"].iloc[0]
    rows = db.upsert_derived("staging", "registry", df, reference_date=acquired_date)
    logger.info("stage_registry: %d rows written", rows)
    return rows


def stage_fees(db: DuckDBWarehouse, force: bool = False) -> int:
    from .fees import fetch_raw_fees

    if not force and _snapshot_current(db, "raw.cad_fi_hist_taxa_adm", "staging.fees"):
        logger.info("stage_fees: already up-to-date, skipping")
        return 0
    df = fetch_raw_fees(db)
    if df is None or df.empty:
        logger.warning("stage_fees: no raw data available")
        return 0
    acquired_date = df["reference_date"].iloc[0]
    rows = db.upsert_derived("staging", "fees", df, reference_date=acquired_date)
    logger.info("stage_fees: %d rows written", rows)
    return rows


def stage_daily_quotes(db: DuckDBWarehouse, force: bool = False) -> int:
    from .daily_quotes import fetch_raw_daily_quotes

    if force:
        db.execute("DELETE FROM staging.daily_quotes")
    df = fetch_raw_daily_quotes(db, force=force)
    if df is None or df.empty:
        logger.info("stage_daily_quotes: no new data")
        return 0
    rows = db.upsert_timeseries("staging", "daily_quotes", df, _DAILY_QUOTES_KEY)
    logger.info("stage_daily_quotes: %d rows written", rows)
    return rows


def stage_cdi_rates(db: DuckDBWarehouse, force: bool = False) -> int:
    from .cdi_rates import fetch_raw_cdi_rates

    if force:
        db.execute("DELETE FROM staging.cdi_rates")
    df = fetch_raw_cdi_rates(db, force=force)
    if df is None or df.empty:
        logger.info("stage_cdi_rates: no new data")
        return 0
    rows = db.upsert_timeseries("staging", "cdi_rates", df, _CDI_RATES_KEY)
    logger.info("stage_cdi_rates: %d rows written", rows)
    return rows


def stage_anbima(db: DuckDBWarehouse, force: bool = False) -> int:
    from .anbima import fetch_raw_anbima

    if not force and _snapshot_current(
        db, "raw.anbima_caracteristicas", "staging.anbima"
    ):
        logger.info("stage_anbima: already up-to-date, skipping")
        return 0
    df = fetch_raw_anbima(db)
    if df is None or df.empty:
        logger.warning("stage_anbima: no raw data available")
        return 0
    acquired_date = df["reference_date"].iloc[0]
    rows = db.upsert_derived("staging", "anbima", df, reference_date=acquired_date)
    logger.info("stage_anbima: %d rows written", rows)
    return rows


def _snapshot_current(
    db: DuckDBWarehouse, raw_dataset: str, staging_dataset: str
) -> bool:
    """True if staging already holds a snapshot >= the latest raw snapshot."""
    raw_max = db.execute(f"SELECT MAX(reference_date) FROM {raw_dataset}").fetchone()[0]
    if raw_max is None:
        return True
    staging_max = db.execute(
        f"SELECT MAX(reference_date) FROM {staging_dataset}"
    ).fetchone()[0]
    return staging_max is not None and staging_max >= raw_max
