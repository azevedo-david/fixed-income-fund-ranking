"""Cached DuckDB query helpers for the Streamlit dashboard."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "fund_ranking.duckdb"


@st.cache_resource
def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def _q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return _con().execute(sql, params).fetchdf()


@st.cache_data(ttl=3600)
def reference_dates() -> list[date]:
    df = _q(
        "SELECT DISTINCT reference_date FROM marts.rankings ORDER BY reference_date DESC"
    )
    return df["reference_date"].tolist()


@st.cache_data(ttl=3600)
def ranking_dims(ref_date: date) -> dict[str, list[str]]:
    df = _q(
        """
        SELECT DISTINCT purpose, profile, investor_type
        FROM marts.rankings
        WHERE reference_date = ?
        """,
        (ref_date,),
    )
    return {
        "purpose": sorted(df["purpose"].dropna().unique().tolist()),
        "profile": sorted(df["profile"].dropna().unique().tolist()),
        "investor_type": sorted(df["investor_type"].dropna().unique().tolist()),
    }


@st.cache_data(ttl=3600)
def rankings(
    ref_date: date,
    purpose: str,
    profile: str,
    investor_type: str,
    top_n: int,
    name_filter: str | None = None,
) -> pd.DataFrame:
    name_clause = "AND fund_name ILIKE ?" if name_filter else ""
    params: tuple = (ref_date, purpose, profile, investor_type)
    if name_filter:
        params = params + (f"%{name_filter}%",)
    params = params + (top_n,)
    return _q(
        f"""
        SELECT
            rank, fund_name, fund_cnpj, subclass_id, score,
            return_12m_net, alpha_12m_net, sharpe_excess, volatility,
            max_drawdown, pct_months_above_cdi, redemption_days, min_investment
        FROM marts.rankings
        WHERE reference_date = ?
          AND purpose = ?
          AND profile = ?
          AND investor_type = ?
          {name_clause}
        ORDER BY rank
        LIMIT ?
        """,
        params,
    )


@st.cache_data(ttl=3600)
def universe_stats(ref_date: date) -> dict[str, float]:
    row = _q(
        """
        SELECT
            COUNT(u.fund_cnpj)               AS n_funds,
            MEDIAN(u.median_aum)             AS median_aum,
            MEDIAN(u.median_holders)         AS median_holders,
            MEDIAN(m.return_annualized_net)  AS median_annualized_return
        FROM marts.universe u
        LEFT JOIN marts.metrics m
          ON m.fund_cnpj = u.fund_cnpj
         AND COALESCE(m.subclass_id, '__NS__') = COALESCE(u.subclass_id, '__NS__')
         AND m.reference_date = u.reference_date
        WHERE u.reference_date = ?
        """,
        (ref_date,),
    ).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=3600)
def fund_header(fund_cnpj: str, subclass_id: str | None, ref_date: date) -> dict:
    sub_clause = (
        "AND subclass_id = ?" if subclass_id is not None else "AND subclass_id IS NULL"
    )
    params: tuple = (fund_cnpj, ref_date)
    if subclass_id is not None:
        params = (fund_cnpj, subclass_id, ref_date)
    df = _q(
        f"""
        SELECT
            u.fund_name, u.anbima_category, u.target_investor, u.target_taxation,
            u.adm_fee, u.has_perf_fee, u.redemption_days, u.min_investment,
            u.median_aum, u.median_holders, u.inception_date, u.share_class,
            u.fund_structure,
            m.return_12m_net, m.alpha_12m_net, m.alpha_6m_net, m.alpha_3m_net,
            m.sharpe_excess, m.volatility, m.max_drawdown,
            m.pct_months_above_cdi, m.return_annualized_net,
            m.ir_rate, m.span_days
        FROM marts.universe u
        LEFT JOIN marts.metrics m
          ON m.fund_cnpj = u.fund_cnpj
         AND COALESCE(m.subclass_id, '__NS__') = COALESCE(u.subclass_id, '__NS__')
         AND m.reference_date = u.reference_date
        WHERE u.fund_cnpj = ?
          {sub_clause.replace('subclass_id', 'u.subclass_id')}
          AND u.reference_date = ?
        """,
        params,
    )
    return df.iloc[0].to_dict() if len(df) else {}


@st.cache_data(ttl=3600)
def fund_rank_row(
    fund_cnpj: str,
    subclass_id: str | None,
    ref_date: date,
    purpose: str,
    profile: str,
    investor_type: str,
) -> dict:
    sub_clause = (
        "AND subclass_id = ?" if subclass_id is not None else "AND subclass_id IS NULL"
    )
    params: tuple = (fund_cnpj, ref_date, purpose, profile, investor_type)
    if subclass_id is not None:
        params = (fund_cnpj, subclass_id, ref_date, purpose, profile, investor_type)
    df = _q(
        f"""
        SELECT rank, score
        FROM marts.rankings
        WHERE fund_cnpj = ?
          {sub_clause}
          AND reference_date = ?
          AND purpose = ?
          AND profile = ?
          AND investor_type = ?
        """,
        params,
    )
    return df.iloc[0].to_dict() if len(df) else {}


@st.cache_data(ttl=3600)
def fund_nav_12m(
    fund_cnpj: str, subclass_id: str | None, ref_date: date
) -> pd.DataFrame:
    """Daily NAV and CDI over the 12 months ending at ref_date.

    CDI calendar is the ground truth: returns are forward-filled to 0 on days
    the fund did not publish a quote.
    """
    sub_clause = (
        "AND q.subclass_id = ?"
        if subclass_id is not None
        else "AND q.subclass_id IS NULL"
    )
    params: tuple
    if subclass_id is not None:
        params = (ref_date, ref_date, fund_cnpj, subclass_id)
    else:
        params = (ref_date, ref_date, fund_cnpj)
    df = _q(
        f"""
        WITH cdi AS (
            SELECT date, rate
            FROM staging.cdi_rates
            WHERE date BETWEEN (? - INTERVAL 12 MONTH) AND ?
        ),
        nav AS (
            SELECT q.date, q.nav
            FROM staging.daily_quotes q
            WHERE q.fund_cnpj = ?
              {sub_clause}
        )
        SELECT
            cdi.date,
            cdi.rate AS cdi_rate,
            nav.nav
        FROM cdi
        LEFT JOIN nav ON nav.date = cdi.date
        ORDER BY cdi.date
        """,
        params,
    )

    df["nav"] = df["nav"].ffill()
    df["fund_ret"] = df["nav"].pct_change().fillna(0.0)
    df["cdi_ret"] = df["cdi_rate"].fillna(0.0)

    fund_level = (1 + df["fund_ret"]).cumprod()
    df["fund_cum"] = fund_level - 1
    df["cdi_cum"] = (1 + df["cdi_ret"]).cumprod() - 1

    running_max = fund_level.cummax()
    df["drawdown"] = fund_level / running_max - 1
    return df


@st.cache_data(ttl=3600)
def cohort_returns(
    ref_date: date, purpose: str, profile: str, investor_type: str
) -> pd.DataFrame:
    return _q(
        """
        SELECT
            fund_cnpj, subclass_id, fund_name, rank, score,
            return_12m_net, return_annualized_net,
            alpha_12m_net, alpha_6m_net, alpha_3m_net,
            sharpe_excess, pct_months_above_cdi,
            max_drawdown, volatility
        FROM marts.rankings
        WHERE reference_date = ?
          AND purpose = ?
          AND profile = ?
          AND investor_type = ?
        """,
        (ref_date, purpose, profile, investor_type),
    )


# ── validation / pipeline debugging ──────────────────────────────────────────


@st.cache_data(ttl=60)
def validation_reference_dates() -> list[date]:
    df = _q(
        "SELECT DISTINCT reference_date FROM logs.validation_log ORDER BY reference_date DESC"
    )
    return df["reference_date"].tolist()


@st.cache_data(ttl=60)
def validation_summary(ref_date: date) -> pd.DataFrame:
    """One row per task with pass / fail / warn counts and last logged timestamp."""
    return _q(
        """
        SELECT
            task,
            dataset,
            COUNT(*)                                              AS total,
            SUM(passed::INT)                                      AS passed,
            COUNT(*) - SUM(passed::INT)                           AS failed,
            SUM(CASE WHEN NOT passed AND severity = 'error'
                     THEN 1 ELSE 0 END)                           AS errors,
            SUM(CASE WHEN NOT passed AND severity = 'warning'
                     THEN 1 ELSE 0 END)                           AS warnings,
            MAX(logged_at)                                        AS last_logged_at
        FROM logs.validation_log
        WHERE reference_date = ?
        GROUP BY task, dataset
        ORDER BY task
        """,
        (ref_date,),
    )


@st.cache_data(ttl=60)
def validation_detail(ref_date: date, task: str) -> pd.DataFrame:
    """All checks for a given task, failures first."""
    return _q(
        """
        SELECT
            check_name, severity, passed, value, threshold, message, logged_at
        FROM logs.validation_log
        WHERE reference_date = ?
          AND task = ?
        ORDER BY passed ASC, severity DESC, check_name
        """,
        (ref_date, task),
    )
