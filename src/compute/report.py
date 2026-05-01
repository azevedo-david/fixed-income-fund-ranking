"""Generate the ranking.md report from metrics_df.

Produces one section per ranking combo defined in ``settings.rankings``,
each showing the top-N funds as a markdown table with formatted metrics.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Settings
from .ranking import rank_funds

logger = logging.getLogger(__name__)

_DISPLAY_COLS = [
    ("fund_name",             "Fund",              lambda v: str(v)),
    ("return_annualized_net", "Ret ann (net)",     lambda v: f"{v:+.2%}"),
    ("alpha_12m_net",         "α 12m (net)",       lambda v: f"{v:+.2%}"),
    ("return_12m_net",        "Ret 12m (net)",     lambda v: f"{v:+.2%}"),
    ("sharpe_excess",         "Sharpe",            lambda v: f"{v:.2f}"),
    ("pct_months_above_cdi",  "% months > CDI",    lambda v: f"{v:.0%}"),
    ("max_drawdown",          "Max DD",            lambda v: f"{v:.2%}"),
    ("redemption_days",       "Redemption",        lambda v: f"D+{v:.0f}"),
    ("volatility",            "Vol (ann)",         lambda v: f"{v:.2%}"),
]

_PURPOSE_LABEL  = {"cash": "Cash", "income": "Income"}
_PROFILE_LABEL  = {"conservative": "Conservative", "balanced": "Balanced", "aggressive": "Aggressive"}
_INVESTOR_LABEL = {"retail": "Retail", "qualified": "Qualified", "professional": "Professional"}

_PURPOSE_DESC = {
    "cash":   "short-duration parking — favours consistency, near-zero drawdown, and same-day or next-day liquidity.",
    "income":  "yield optimisation — favours alpha over CDI, risk-adjusted return, and sustained outperformance.",
}
_PROFILE_DESC = {
    "conservative": "Puts the heaviest weight on % months above CDI and max drawdown; alpha matters but never at the cost of stability.",
    "balanced":     "Balanced weight across alpha, Sharpe, consistency, and drawdown.",
    "aggressive":   "Tilts strongly toward 12-month alpha and annualised return; accepts higher drawdown and lower liquidity.",
}
_INVESTOR_DESC = {
    "retail":       "Accessible to any investor. Applies a minimum-investment penalty (5% of score) favouring funds with lower entry barriers.",
    "qualified":    "Investors with ≥ R$1M in financial investments (CVM definition). No access penalty applied.",
    "professional": "Investors with ≥ R$10M. Includes exclusive and restricted funds unavailable to retail/qualified.",
}

_FEATURE_DESC = {
    "alpha_12m_net":         "12-month net alpha vs CDI",
    "alpha_3m_net":          "3-month net alpha vs CDI",
    "alpha_6m_net":          "6-month net alpha vs CDI",
    "alpha_24m_net":         "24-month net alpha vs CDI",
    "alpha_36m_net":         "36-month net alpha vs CDI",
    "return_annualized_net": "Annualised net return (CAGR)",
    "sharpe_excess":         "Excess Sharpe (excess return / volatility)",
    "pct_months_above_cdi":  "% of calendar months the fund beat CDI",
    "max_drawdown":          "Max peak-to-trough drawdown (higher = smaller loss)",
    "volatility":            "Daily return volatility, annualised (lower is better)",
}


def _methodology_section(settings: Settings) -> list[str]:
    sc = settings.scoring
    u  = settings.universe

    lines: list[str] = [
        "## Methodology",
        "",
        "### Universe",
        "",
        f"Active CVM-registered open-end Renda Fixa funds (RCVM 175) with:",
        f"- Median AuM ≥ R${u.min_aum:,.0f} and ≥ {u.min_cotistas} holders over the last {u.aum_lookback_days} days",
        f"- At least {u.min_span_days} days of NAV history",
        "- Non-exclusive, non-pension, non-closed-end",
        "",
        "### Scoring",
        "",
        "Each fund receives a score in [0, 1] computed as a weighted sum of ten components:",
        "",
        "| # | Component | Description | Direction |",
        "|---|-----------|-------------|-----------|",
    ]
    for i, fs in enumerate(sc.cont_features, 1):
        desc = _FEATURE_DESC.get(fs.col, fs.col)
        direction = "↑ higher is better" if fs.ascending else "↓ lower is better"
        lines.append(f"| {i} | `{fs.col}` | {desc} | {direction} |")
    lines += [
        f"| {len(sc.cont_features)+1} | `s_span` | Fund longevity: `1 − exp(−{sc.span_lambda} × span_years)` | ↑ |",
        f"| {len(sc.cont_features)+2} | `s_liquidity` | Redemption speed penalty (purpose-dependent λ) | ↑ faster is better |",
        "",
        "Continuous metrics are converted to **percentile ranks** within the eligible universe, "
        "so scores reflect relative standing rather than absolute levels. "
        "`s_span` and `s_liquidity` are structural exponential scores, not percentile-based.",
        "",
        "For **retail** segments an additional accessibility score "
        f"(`s_accessibility = exp(−min_investment / {sc.accessibility_scale:,.0f})`) "
        f"is blended in at {sc.accessibility_weight:.0%} weight, "
        "rescaling the other weights proportionally.",
        "",
        "### Customer segments",
        "",
        "Fixed-income funds span a wide range of management styles — from ultra-liquid DI "
        "reference funds to long-duration inflation-linked debenture portfolios. "
        "The reason an investor is looking for a fund and their risk tolerance matter "
        "enormously: a fund that is ideal for parking cash tomorrow is a poor choice for "
        "building long-term income, and vice versa. "
        "That is why rankings are segmented by **purpose × risk profile × investor type** "
        "rather than producing a single one-size-fits-all list.",
        "",
        "**Purpose** — what the allocation is for:",
    ]
    for k, v in _PURPOSE_DESC.items():
        lines.append(f"- **{_PURPOSE_LABEL[k]}**: {v}")
    lines += ["", "**Risk profile** — how the 10 components are weighted:"]
    for k, v in _PROFILE_DESC.items():
        lines.append(f"- **{_PROFILE_LABEL[k]}**: {v}")
    lines += ["", "**Investor type** — access level and eligibility:"]
    for k, v in _INVESTOR_DESC.items():
        lines.append(f"- **{_INVESTOR_LABEL[k]}**: {v}")

    # Weight table
    lines += [
        "",
        "### Weight vectors",
        "",
        "Each weight vector sums to 1.0. Columns: "
        + ", ".join(f.col.replace("_net", "").replace("return_annualized", "cagr") for f in sc.cont_features)
        + ", span, liquidity.",
        "",
        "| Purpose | Profile | " + " | ".join(
            f.col.replace("alpha_", "α").replace("m_net", "m").replace("return_annualized_net", "cagr")
            .replace("sharpe_excess", "sharpe").replace("pct_months_above_cdi", "%>cdi")
            .replace("max_drawdown", "mdd").replace("volatility", "vol")
            for f in sc.cont_features
        ) + " | span | liq |",
        "|---------|---------|" + "|".join("---" for _ in range(len(sc.cont_features) + 2)) + "|",
    ]
    for purpose, profiles in sc.weights.items():
        for profile, w in profiles.items():
            row = f"| {_PURPOSE_LABEL.get(purpose, purpose)} | {_PROFILE_LABEL.get(profile, profile)} | "
            row += " | ".join(f"{wi:.2f}" for wi in w) + " |"
            lines.append(row)

    lines += ["", "---", ""]
    return lines


def _md_table(ranked: pd.DataFrame, top_n: int) -> str:
    cols = [(col, hdr, fmt) for col, hdr, fmt in _DISPLAY_COLS if col in ranked.columns]
    header = "| # | CNPJ | " + " | ".join(hdr for _, hdr, _ in cols) + " |"
    sep    = "|---|---|" + "|".join("---" for _ in cols) + "|"
    rows   = [header, sep]
    for rank_i, (idx, row) in enumerate(ranked.head(top_n).iterrows(), 1):
        cnpj = idx[0] if isinstance(idx, tuple) else idx
        cells = " | ".join(fmt(row[col]) for col, _, fmt in cols)
        rows.append(f"| {rank_i} | {cnpj} | {cells} |")
    return "\n".join(rows)


def generate_report(metrics_df: pd.DataFrame, settings: Settings) -> str:
    """Rank funds for every combo in ``settings.rankings`` and write ranking.md.

    Returns the markdown string (also written to ``settings.output.ranking_md``).
    """
    ref = str(settings.reference_date)
    lines: list[str] = [
        f"# Fixed Income Fund Ranking — {ref}",
        "",
        f"> Reference date: **{ref}**  ·  Universe: **{len(metrics_df)} funds**",
        "",
    ]
    lines += _methodology_section(settings)
    lines += ["## Rankings", ""]

    for combo in settings.rankings:
        ranked = rank_funds(metrics_df, combo.purpose, settings, combo.profile, combo.investor_type)
        purpose_lbl  = _PURPOSE_LABEL.get(combo.purpose,       combo.purpose.title())
        profile_lbl  = _PROFILE_LABEL.get(combo.profile,       combo.profile.title())
        investor_lbl = _INVESTOR_LABEL.get(combo.investor_type, combo.investor_type.title())

        lines += [
            f"## {purpose_lbl} · {profile_lbl} · {investor_lbl}",
            "",
            f"*{len(ranked)} eligible funds*",
            "",
        ]

        if ranked.empty:
            lines += ["*No eligible funds.*", ""]
        else:
            lines += [_md_table(ranked, settings.top_n), ""]

    md = "\n".join(lines)

    out_path = Path(settings.output.ranking_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    logger.info("ranking.md written (%d segments)", len(settings.rankings))

    return md
