"""Build rankings from marts.metrics."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ..config import RankingCombo, Settings

logger = logging.getLogger(__name__)

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

    required_cols = {fs.col for fs in settings.scoring.cont_features} | {
        "redemption_days",
        "min_investment",
        "investor_level",
        "span_days",
    }
    missing = required_cols - set(df_enriched.columns)
    if missing:
        raise ValueError(f"metrics missing required columns: {missing}")

    sc = settings.scoring
    access = _INVESTOR_ACCESS[investor_type]

    eligible = df_enriched[df_enriched["investor_level"] <= access].copy()
    eligible["redemption_days"] = eligible["redemption_days"].fillna(100)
    if eligible.empty:
        return pd.DataFrame()

    for feat_spec in sc.cont_features:
        col = feat_spec.col
        pct = eligible[col].rank(pct=True, na_option="keep").fillna(0.5)
        eligible[f"s_{col}"] = pct if feat_spec.ascending else (1.0 - pct)

    eligible["s_span"] = 1.0 - np.exp(-sc.span_lambda * eligible["span_days"] / 365.0)
    lam_prazo = sc.liquidity_lambda[purpose]
    eligible["s_liquidity"] = np.exp(-lam_prazo * eligible["redemption_days"])
    eligible["s_accessibility"] = eligible["min_investment"].apply(
        lambda x: np.exp(-x / sc.accessibility_scale) if pd.notna(x) else np.nan
    )

    w = sc.weights[purpose][profile]
    score_cols = [f"s_{fs.col}" for fs in sc.cont_features] + ["s_span", "s_liquidity"]

    if len(w) != len(score_cols):
        raise ValueError(
            f"weights[{purpose}][{profile}] has {len(w)} elements, "
            f"expected {len(score_cols)}"
        )

    if investor_type == "retail":
        scale = 1.0 - sc.accessibility_weight
        eligible["score"] = sum(
            scale * wi * eligible[sc_col] for wi, sc_col in zip(w, score_cols)
        ) + sc.accessibility_weight * eligible["s_accessibility"].fillna(0.5)
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
    out_cols = list(dict.fromkeys(out_cols))
    return eligible[[c for c in out_cols if c in eligible.columns]]


def rank_all(
    metrics_df: pd.DataFrame, settings: Settings
) -> list[tuple[RankingCombo, pd.DataFrame]]:
    """Rank funds for every combo in settings.rankings; returns results in definition order."""
    return [
        (
            combo,
            rank_funds(
                metrics_df, combo.purpose, settings, combo.profile, combo.investor_type
            ),
        )
        for combo in settings.rankings
    ]


def build_rankings(metrics_df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """Rank funds for every combo in settings.rankings; return flat DataFrame."""
    df = metrics_df.rename(columns={"fund_cnpj": "cnpj"})

    results = rank_all(df, settings)

    rows = []
    for combo, ranked in results:
        if ranked.empty:
            logger.warning(
                "rankings: no results for %s/%s/%s",
                combo.purpose,
                combo.profile,
                combo.investor_type,
            )
            continue
        r = ranked.reset_index()
        r["purpose"] = combo.purpose
        r["profile"] = combo.profile
        r["investor_type"] = combo.investor_type
        rows.append(r)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True).rename(columns={"cnpj": "fund_cnpj"})
    logger.info("rankings: %d rows across %d combos", len(out), len(rows))
    return out
