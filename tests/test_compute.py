"""Tests for compute layer: returns, risk, and fund metrics."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.marts.compute.returns import (
    GROUP_KEY,
    annualized_return,
    daily_returns,
    monthly_returns,
    pct_months_above_cdi,
    trailing_returns,
)
from src.marts.compute.risk import max_drawdown, volatility_and_sharpe
from src.marts.metrics import (
    _cdi_annualised,
    compute_fund_metrics,
    map_investor_level,
    span_days,
)

CNPJ_A = "11111111000111"
CNPJ_B = "22222222000122"
N_DAYS = 252


def _make_quotes(n_days: int = N_DAYS) -> pd.DataFrame:
    """Raw quote DataFrame: 2 funds, one with NaN subclass_id."""
    dates = pd.bdate_range("2023-01-02", periods=n_days)
    nav_a = 100.0 * (1.0005 ** np.arange(n_days))
    nav_b = 200.0 * (1.0002 ** np.arange(n_days))
    return pd.concat(
        [
            pd.DataFrame(
                {"cnpj": CNPJ_A, "subclass_id": np.nan, "date": dates, "nav": nav_a}
            ),
            pd.DataFrame(
                {"cnpj": CNPJ_B, "subclass_id": "sub1", "date": dates, "nav": nav_b}
            ),
        ],
        ignore_index=True,
    )


def _make_ri(n_days: int = N_DAYS) -> pd.DataFrame:
    """Daily-returns DataFrame as consumed by compute_fund_metrics."""
    return daily_returns(_make_quotes(n_days))


def _make_cdi(n_days: int = N_DAYS, rate: float = 0.0001) -> pd.Series:
    dates = pd.bdate_range("2023-01-02", periods=n_days)
    return pd.Series(rate, index=dates, name="cdi_daily")


# ---------------------------------------------------------------------------
# daily_returns
# ---------------------------------------------------------------------------


def test_daily_returns_output_columns():
    ri = daily_returns(_make_quotes(10))
    assert set(GROUP_KEY + ["date", "nav", "return_daily"]).issubset(ri.columns)


def test_daily_returns_no_nan_or_inf():
    ri = daily_returns(_make_quotes(20))
    assert ri["return_daily"].notna().all()
    assert not np.isinf(ri["return_daily"]).any()


def test_daily_returns_drops_inf_from_zero_nav():
    quotes = pd.DataFrame(
        {
            "cnpj": ["A", "A", "A"],
            "subclass_id": [np.nan, np.nan, np.nan],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "nav": [0.0, 1.0, 2.0],
        }
    )
    ri = daily_returns(quotes)
    assert not np.isinf(ri["return_daily"]).any()
    assert len(ri) < 3


def test_daily_returns_both_funds_present():
    ri = daily_returns(_make_quotes(20))
    assert set(ri["cnpj"].unique()) == {CNPJ_A, CNPJ_B}


# ---------------------------------------------------------------------------
# trailing_returns
# ---------------------------------------------------------------------------


def _ref(ri: pd.DataFrame) -> date:
    return ri["date"].max().date()


def test_trailing_returns_columns():
    ri = _make_ri()
    result = trailing_returns(ri, {"3m": 3, "12m": 12}, _ref(ri))
    assert {"return_3m", "return_12m"}.issubset(result.columns)
    assert set(result["cnpj"].unique()) == {CNPJ_A, CNPJ_B}


def test_trailing_returns_nan_subclass_preserved():
    ri = _make_ri()
    result = trailing_returns(ri, {"12m": 12}, _ref(ri))
    fund_a = result[result["cnpj"] == CNPJ_A]
    assert len(fund_a) == 1
    assert pd.isna(fund_a.iloc[0]["subclass_id"])


def test_trailing_returns_positive_for_growing_nav():
    ri = _make_ri()
    result = trailing_returns(ri, {"12m": 12}, _ref(ri))
    assert (result["return_12m"].dropna() > 0).all()


# ---------------------------------------------------------------------------
# annualized_return
# ---------------------------------------------------------------------------


def test_annualized_return_two_rows():
    result = annualized_return(_make_ri())
    assert len(result) == 2
    assert "annualized_return" in result.columns


def test_annualized_return_positive():
    result = annualized_return(_make_ri())
    assert (result["annualized_return"] > 0).all()


def test_annualized_return_approx():
    # fund A: 0.05%/day; CAGR over ~252 bdays ≈ 13-14%
    result = annualized_return(_make_ri())
    a = result[result["cnpj"] == CNPJ_A]["annualized_return"].iloc[0]
    assert 0.10 < a < 0.20


# ---------------------------------------------------------------------------
# span_days
# ---------------------------------------------------------------------------


def test_span_days_two_rows():
    result = span_days(_make_ri(10))
    assert len(result) == 2
    assert (result["span_days"] > 0).all()


# ---------------------------------------------------------------------------
# volatility_and_sharpe
# ---------------------------------------------------------------------------


def test_volatility_and_sharpe_shape():
    result = volatility_and_sharpe(_make_ri(), _make_cdi())
    assert len(result) == 2
    assert {"volatility", "sharpe_excess", "sharpe_raw"}.issubset(result.columns)


def test_volatility_positive():
    result = volatility_and_sharpe(_make_ri(), _make_cdi())
    assert (result["volatility"] > 0).all()


# ---------------------------------------------------------------------------
# max_drawdown
# ---------------------------------------------------------------------------


def test_max_drawdown_shape():
    result = max_drawdown(_make_ri())
    assert len(result) == 2
    assert "max_drawdown" in result.columns


def test_max_drawdown_zero_for_monotone_nav():
    result = max_drawdown(_make_ri())
    assert (result["max_drawdown"] >= -1e-10).all()


def test_max_drawdown_negative_after_drop():
    # single fund that peaks then drops
    dates = pd.bdate_range("2023-01-02", periods=20)
    nav = np.concatenate([np.linspace(100, 120, 10), np.linspace(120, 90, 10)])
    ri = pd.DataFrame(
        {
            "cnpj": "XTEST",
            "subclass_id": np.nan,
            "date": dates,
            "nav": nav,
            "return_daily": np.diff(nav, prepend=nav[0])
            / np.concatenate([[nav[0]], nav[:-1]]),
        }
    )
    result = max_drawdown(ri)
    assert result["max_drawdown"].iloc[0] < -0.1


# ---------------------------------------------------------------------------
# monthly_returns / pct_months_above_cdi
# ---------------------------------------------------------------------------


def test_monthly_returns_columns():
    cdi = _make_cdi()
    result = monthly_returns(_make_ri(), cdi)
    assert {"cnpj", "subclass_id", "month", "r_fund", "r_cdi"}.issubset(result.columns)


def test_pct_months_above_cdi_range():
    cdi = _make_cdi()
    monthly = monthly_returns(_make_ri(), cdi)
    result = pct_months_above_cdi(monthly)
    assert (
        (result["pct_months_above_cdi"] >= 0) & (result["pct_months_above_cdi"] <= 1)
    ).all()


# ---------------------------------------------------------------------------
# map_investor_level
# ---------------------------------------------------------------------------


def test_map_investor_level_geral_is_zero():
    assert map_investor_level(pd.Series(["Investidor Geral"])).iloc[0] == 0


def test_map_investor_level_qualificado_is_one():
    assert map_investor_level(pd.Series(["Qualificado"])).iloc[0] == 1


def test_map_investor_level_profissional_is_two():
    assert map_investor_level(pd.Series(["Profissional"])).iloc[0] == 2


def test_map_investor_level_none_is_zero():
    assert map_investor_level(pd.Series([None])).iloc[0] == 0


def test_map_investor_level_case_insensitive():
    levels = map_investor_level(pd.Series(["PROFISSIONAL", "QUALIFICADO"]))
    assert list(levels) == [2, 1]


# ---------------------------------------------------------------------------
# _cdi_annualised
# ---------------------------------------------------------------------------


def test_cdi_annualised_empty_returns_zero():
    empty = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))
    assert _cdi_annualised(empty, pd.Timestamp("2024-12-31")) == 0.0


def test_cdi_annualised_positive_for_positive_rates():
    cdi = _make_cdi()
    result = _cdi_annualised(cdi, cdi.index.max())
    assert result > 0


# ---------------------------------------------------------------------------
# compute_fund_metrics (end-to-end)
# ---------------------------------------------------------------------------


def test_compute_fund_metrics_two_rows():
    ri = _make_ri()
    cdi = _make_cdi()
    ref = cdi.index.max().date()
    result = compute_fund_metrics(ri, cdi, {"3m": 3, "12m": 12}, ref)
    assert len(result) == 2


def test_compute_fund_metrics_expected_columns():
    ri = _make_ri()
    cdi = _make_cdi()
    ref = cdi.index.max().date()
    result = compute_fund_metrics(ri, cdi, {"3m": 3, "12m": 12}, ref)
    expected = {
        "cnpj",
        "subclass_id",
        "return_3m",
        "return_12m",
        "alpha_3m",
        "alpha_12m",
        "annualized_return",
        "pct_months_above_cdi",
        "volatility",
        "sharpe_excess",
        "sharpe_raw",
        "max_drawdown",
        "span_days",
    }
    assert expected.issubset(result.columns)


def test_compute_fund_metrics_nan_subclass_preserved():
    ri = _make_ri()
    cdi = _make_cdi()
    ref = cdi.index.max().date()
    result = compute_fund_metrics(ri, cdi, {"12m": 12}, ref)
    fund_a = result[result["cnpj"] == CNPJ_A]
    assert len(fund_a) == 1
    assert pd.isna(fund_a.iloc[0]["subclass_id"])
