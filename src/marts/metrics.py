"""Compute fund metrics from staging tables."""

from __future__ import annotations

import logging
from dataclasses import replace as _replace
from datetime import date

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from .compute.returns import cdi_window_returns, daily_returns
from .compute.metrics import (
    _SUB_SENTINEL,
    _apply_tax_layer,
    _build_indexed_returns,
    _cdi_annualised,
    _compute_per_fund_metrics,
    filter_min_span,
    map_investor_level,
)
from ..config import Settings
from ..storage import DuckDBWarehouse

logger = logging.getLogger(__name__)


def build_metrics(
    db: DuckDBWarehouse,
    universe_df: pd.DataFrame,
    reference_date: date,
    settings: Settings,
) -> pd.DataFrame:
    """Build metrics DataFrame from staging.daily_quotes and staging.cdi_rates."""
    settings = _replace(settings, reference_date=reference_date)

    cdi_start = (
        pd.Timestamp(reference_date)
        - relativedelta(months=settings.max_window_months + 2)
    ).date()

    db._con.register(
        "_metrics_universe",
        universe_df[["fund_cnpj", "subclass_id"]].drop_duplicates(),
    )
    try:
        quotes_df = db.execute(
            """
            SELECT dq.fund_cnpj AS cnpj, dq.subclass_id, dq.date, dq.nav
            FROM staging.daily_quotes dq
            INNER JOIN _metrics_universe u
                ON dq.fund_cnpj = u.fund_cnpj
               AND dq.subclass_id IS NOT DISTINCT FROM u.subclass_id
            WHERE dq.date >= ? AND dq.date <= ?
            """,
            [settings.quotes_start, reference_date],
        ).df()
    finally:
        db._con.unregister("_metrics_universe")

    cdi_df = db.execute(
        "SELECT date, rate FROM staging.cdi_rates WHERE date >= ? AND date <= ? ORDER BY date",
        [cdi_start, reference_date],
    ).df()
    cdi = pd.Series(
        cdi_df["rate"].values,
        index=pd.to_datetime(cdi_df["date"]),
        name="cdi_daily",
    )

    daily = daily_returns(quotes_df)
    ri = _build_indexed_returns(daily, cdi)
    ri = filter_min_span(ri, settings.universe.min_span_days)
    n_funds = ri.index.droplevel("date").unique().shape[0]
    logger.info("metrics: %d funds after span filter for %s", n_funds, reference_date)

    metrics = _compute_per_fund_metrics(ri, cdi, settings)

    ref = pd.Timestamp(reference_date)
    cdi_window = cdi_window_returns(cdi, ref, settings.windows)
    cdi_annual = _cdi_annualised(cdi, ref)

    df = metrics.reset_index()
    df["subclass_id"] = df["subclass_id"].fillna(_SUB_SENTINEL)

    meta_cols = [
        "fund_cnpj",
        "subclass_id",
        "fund_name",
        "target_investor",
        "target_taxation",
        "redemption_days",
        "min_investment",
    ]
    meta = (
        universe_df[meta_cols]
        .copy()
        .assign(cnpj=lambda x: x["fund_cnpj"])
        .fillna({"subclass_id": _SUB_SENTINEL})
        .drop_duplicates(subset=["cnpj", "subclass_id"])
    )
    df = df.merge(
        meta.drop(columns=["fund_cnpj"]), on=["cnpj", "subclass_id"], how="left"
    )
    df["subclass_id"] = df["subclass_id"].replace(_SUB_SENTINEL, np.nan)

    df = _apply_tax_layer(df, cdi_window, cdi_annual, settings)
    df["investor_level"] = map_investor_level(df["target_investor"])

    df = df.rename(columns={"cnpj": "fund_cnpj"})
    df["reference_date"] = reference_date
    logger.info("metrics: %d rows computed for %s", len(df), reference_date)
    return df
