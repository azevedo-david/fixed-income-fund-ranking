"""Fund detail page: 12m return vs CDI, drawdown, KPIs, cohort context."""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import data

# ── green palette ────────────────────────────────────────────────────────────
GREEN_DARK = "#2d6a4f"
GREEN_LIGHT = "#95d5b2"
GREEN_MID = "#52b788"
GREEN_FILL = "#b7e4c7"

# ── ranking metrics available for cohort context ─────────────────────────────
# (label, column, higher_is_better)
COHORT_METRICS: list[tuple[str, str, bool]] = [
    ("Return 12m (net)", "return_12m_net", True),
    ("Return annualized (net)", "return_annualized_net", True),
    ("Alpha 12m (net)", "alpha_12m_net", True),
    ("Alpha 6m (net)", "alpha_6m_net", True),
    ("Alpha 3m (net)", "alpha_3m_net", True),
    ("Sharpe (excess)", "sharpe_excess", True),
    ("% months > CDI", "pct_months_above_cdi", True),
    ("Volatility", "volatility", False),
    ("Max drawdown", "max_drawdown", False),
]
METRIC_LABELS = [m[0] for m in COHORT_METRICS]
METRIC_BY_LABEL = {m[0]: (m[1], m[2]) for m in COHORT_METRICS}


def _pct(v) -> str:
    return f"{v:.2%}" if v is not None and v == v else "—"


def _num(v, fmt=",.0f") -> str:
    return format(v, fmt) if v is not None and v == v else "—"


def _date(v) -> str:
    if v is None or v != v:
        return "—"
    return str(v)[:10]


# ── session guard ─────────────────────────────────────────────────────────────
sel = st.session_state.get("selected_fund")
if not sel:
    st.info("Pick a fund from the Rankings page to see its detail.")
    if st.button("Back to Rankings"):
        st.switch_page("pages/1_Rankings.py")
    st.stop()

cnpj = sel["fund_cnpj"]
sub = sel["subclass_id"]
ref_date = sel["reference_date"]
purpose = sel["purpose"]
profile = sel["profile"]
investor_type = sel["investor_type"]

if st.button("← Back to Rankings"):
    st.switch_page("pages/1_Rankings.py")

header = data.fund_header(cnpj, sub, ref_date)
if not header:
    st.error("Fund not found for this reference date.")
    st.stop()

rank_row = data.fund_rank_row(cnpj, sub, ref_date, purpose, profile, investor_type)

# ── header ────────────────────────────────────────────────────────────────────
st.title(header["fund_name"])
st.caption(
    f"CNPJ {cnpj} · {header['anbima_category']} · {header['target_investor']} · "
    f"taxation: {header['target_taxation']} · inception {_date(header['inception_date'])}"
)
st.caption(
    f"Cohort: {purpose} · {profile} · {investor_type} · ref {ref_date.strftime('%Y-%m-%d')}"
)

# ── KPIs ──────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Rank", f"#{int(rank_row['rank'])}" if rank_row else "—")
k2.metric("Score", f"{rank_row['score']:.3f}" if rank_row else "—")
k3.metric("Return 12m (net)", _pct(header.get("return_12m_net")))
k4.metric("Alpha 12m (net)", _pct(header.get("alpha_12m_net")))
k5.metric(
    "Sharpe (excess)",
    (
        f"{header['sharpe_excess']:.2f}"
        if header.get("sharpe_excess") == header.get("sharpe_excess")
        else "—"
    ),
)

k6, k7, k8, k9, k10 = st.columns(5)
k6.metric("Volatility", _pct(header.get("volatility")))
k7.metric("Max drawdown", _pct(header.get("max_drawdown")))
k8.metric("% months > CDI", _pct(header.get("pct_months_above_cdi")))
adm = header.get("adm_fee")
k9.metric("Adm fee", _pct((adm / 100) if adm and adm > 1 else adm))
k10.metric("Redemption (d)", _num(header.get("redemption_days")))

# ── 12m return chart ──────────────────────────────────────────────────────────
st.subheader("12-month cumulative return vs CDI")
nav = data.fund_nav_12m(cnpj, sub, ref_date)
if nav.empty or nav["nav"].notna().sum() == 0:
    st.warning("No NAV data available for this fund in the last 12 months.")
else:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=nav["date"],
            y=nav["fund_cum"],
            name="Fund",
            line=dict(color=GREEN_DARK, width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=nav["date"],
            y=nav["cdi_cum"],
            name="CDI",
            line=dict(color=GREEN_LIGHT, width=2, dash="dash"),
        )
    )
    fig.update_layout(
        height=420,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis_title="Cumulative return",
        yaxis_tickformat=".1%",
        legend=dict(orientation="h", y=1.05),
        template="plotly_dark",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Drawdown (12m)")
    dd_fig = go.Figure()
    dd_fig.add_trace(
        go.Scatter(
            x=nav["date"],
            y=nav["drawdown"],
            fill="tozeroy",
            fillcolor=GREEN_FILL,
            line=dict(color=GREEN_DARK),
            name="Drawdown",
        )
    )
    dd_fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis_tickformat=".1%",
        template="plotly_dark",
    )
    st.plotly_chart(dd_fig, use_container_width=True)

# ── cohort context ────────────────────────────────────────────────────────────
st.subheader("Cohort context")
cohort = data.cohort_returns(ref_date, purpose, profile, investor_type)

if cohort.empty:
    st.info("No cohort data.")
else:
    metric_label = st.selectbox("Metric", METRIC_LABELS, index=0, key="cohort_metric")
    col_name, higher_is_better = METRIC_BY_LABEL[metric_label]

    fund_val = header.get(col_name)
    valid = cohort[col_name].dropna()

    if fund_val is not None and fund_val == fund_val and len(valid) > 0:
        if higher_is_better:
            pct_rank = (valid < fund_val).mean() * 100
        else:
            pct_rank = (valid > fund_val).mean() * 100
        pct_label = f"**{pct_rank:.0f}th percentile** in cohort ({len(valid)} funds)"
    else:
        pct_label = "Percentile unavailable"

    st.markdown(pct_label)

    is_pct_axis = col_name not in ("sharpe_excess",)
    hist = px.histogram(
        cohort.dropna(subset=[col_name]),
        x=col_name,
        nbins=40,
        color_discrete_sequence=[GREEN_MID],
        labels={col_name: metric_label},
    )
    hist.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=20, b=10),
        xaxis_tickformat=".1%" if is_pct_axis else ".2f",
        template="plotly_dark",
        showlegend=False,
    )
    if fund_val is not None and fund_val == fund_val:
        hist.add_shape(
            type="line",
            x0=fund_val,
            x1=fund_val,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color=GREEN_LIGHT, dash="dash", width=2),
        )
        hist.add_annotation(
            x=fund_val,
            y=1,
            yref="paper",
            text="This fund",
            showarrow=False,
            yanchor="bottom",
            xanchor="left",
            xshift=6,
            font=dict(color=GREEN_LIGHT, size=12),
        )
    st.plotly_chart(hist, use_container_width=True)

with st.expander("All metrics"):
    st.json({k: (None if v != v else v) for k, v in header.items()})
