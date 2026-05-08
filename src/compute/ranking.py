"""Percentile-based scoring and ranking of fixed-income funds."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ..config import Settings

_INVESTOR_ACCESS = {"retail": 0, "qualified": 1, "professional": 2}


_OUT_COLS = [
    "fund_name",
    "investor_level",
    "ir_rate",
    "alpha_12m_net",
    "alpha_3m_net",
    "alpha_6m_net",
    "return_annualized_net",
    "sharpe_excess",
    "pct_months_above_cdi",
    "max_drawdown",
    "volatility",
    "span_days",
    "redemption_days",
    "min_investment",
    "return_12m_net",
    "score",
    "rank",
]


def rank_funds(
    df_enriched: pd.DataFrame,
    purpose: str,
    settings: Settings,
    profile: str = "balanced",
    investor_type: str = "qualified",
) -> pd.DataFrame:
    """Score and rank funds for a given purpose × profile × investor_type combination."""
    if purpose not in settings.scoring.weights:
        raise ValueError(f"unknown purpose {purpose!r}")
    if profile not in settings.scoring.weights[purpose]:
        raise ValueError(f"unknown profile {profile!r}")
    if investor_type not in _INVESTOR_ACCESS:
        raise ValueError(f"investor_type must be one of {list(_INVESTOR_ACCESS)}")

    sc = settings.scoring
    access = _INVESTOR_ACCESS[investor_type]

    eligible = (
        df_enriched[df_enriched["investor_level"] <= access]
        .dropna(subset=["redemption_days"])
        .copy()
    )
    if eligible.empty:
        return pd.DataFrame()

    # NaN → 0.5 (neutral rank): missing window data is neither penalised nor rewarded.
    for feat_spec in sc.cont_features:
        col = feat_spec.col
        pct = eligible[col].rank(pct=True, na_option="keep").fillna(0.5)
        eligible[f"s_{col}"] = pct if feat_spec.ascending else (1.0 - pct)

    eligible["s_span"] = 1.0 - np.exp(-sc.span_lambda * eligible["span_days"] / 365.0)
    lam_prazo = sc.liquidity_lambda[purpose]
    eligible["s_liquidity"] = np.exp(-lam_prazo * eligible["redemption_days"])
    eligible["s_accessibility"] = np.exp(
        -eligible["min_investment"].fillna(0) / sc.accessibility_scale
    )

    w = sc.weights[purpose][profile]
    score_cols = [f"s_{fs.col}" for fs in sc.cont_features] + ["s_span", "s_liquidity"]

    if investor_type == "retail":
        scale = 1.0 - sc.accessibility_weight
        eligible["score"] = (
            sum(scale * wi * eligible[sc_col] for wi, sc_col in zip(w, score_cols))
            + sc.accessibility_weight * eligible["s_accessibility"]
        )
    else:
        eligible["score"] = sum(
            wi * eligible[sc_col] for wi, sc_col in zip(w, score_cols)
        )

    eligible = eligible.sort_values("score", ascending=False)
    eligible["rank"] = eligible["score"].rank(method="min", ascending=False).astype(int)

    extra_score_cols = [f"s_{fs.col}" for fs in sc.cont_features] + [
        "s_span",
        "s_liquidity",
        "s_accessibility",
    ]
    eligible = eligible.set_index(["cnpj", "subclass_id"])

    out_cols = _OUT_COLS + extra_score_cols
    out_cols = list(dict.fromkeys(out_cols))  # deduplicate, preserve order
    return eligible[[c for c in out_cols if c in eligible.columns]]
