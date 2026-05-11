"""Landing page: filter rankings by reference_date + purpose × profile × investor_type."""

import streamlit as st

import data

st.title("Fixed Income Fund Rankings")

ref_dates = data.reference_dates()
if not ref_dates:
    st.warning(
        "No rankings found. Run the pipeline first to populate `marts.rankings`."
    )
    st.stop()


def _default_idx(lst: list, val: str) -> int:
    try:
        return lst.index(val)
    except ValueError:
        return 0


with st.sidebar:
    st.header("Filters")
    ref_date = st.selectbox(
        "Reference date",
        ref_dates,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
    )
    dims = data.ranking_dims(ref_date)
    purpose = st.selectbox(
        "Purpose", dims["purpose"], index=_default_idx(dims["purpose"], "cash")
    )
    profile = st.selectbox(
        "Risk profile",
        dims["profile"],
        index=_default_idx(dims["profile"], "conservative"),
    )
    investor_type = st.selectbox(
        "Investor type",
        dims["investor_type"],
        index=_default_idx(dims["investor_type"], "retail"),
    )
    top_n = st.slider("Top N", min_value=5, max_value=30, value=5, step=5)
    name_filter = st.text_input("Search name", value="")

stats = data.universe_stats(ref_date)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Funds in universe", f"{int(stats['n_funds']):,}")
c2.metric("Median AuM (BRL)", f"{stats['median_aum']:,.0f}")
c3.metric("Median holders", f"{stats['median_holders']:,.0f}")
med_ret = stats.get("median_annualized_return")
c4.metric(
    "Median annualized return",
    f"{med_ret:.2%}" if med_ret is not None and med_ret == med_ret else "—",
)

st.subheader(f"{purpose} · {profile} · {investor_type}")

df = data.rankings(
    ref_date, purpose, profile, investor_type, top_n, name_filter or None
)

if df.empty:
    st.info("No funds match the current filters.")
    st.stop()

display = df.copy()
for col in [
    "return_12m_net",
    "alpha_12m_net",
    "max_drawdown",
    "volatility",
    "pct_months_above_cdi",
]:
    display[col] = display[col].map(lambda v: f"{v:.2%}" if v == v else "—")
display["score"] = display["score"].map(lambda v: f"{v:.3f}" if v == v else "—")
display["sharpe_excess"] = display["sharpe_excess"].map(
    lambda v: f"{v:.2f}" if v == v else "—"
)
display["min_investment"] = display["min_investment"].map(
    lambda v: f"{v:,.0f}" if v == v else "—"
)
display = display.rename(
    columns={
        "rank": "Rank",
        "fund_name": "Fund",
        "score": "Score",
        "return_12m_net": "Return 12m (net)",
        "alpha_12m_net": "Alpha 12m (net)",
        "sharpe_excess": "Sharpe",
        "volatility": "Vol",
        "max_drawdown": "Max DD",
        "pct_months_above_cdi": "% months > CDI",
        "redemption_days": "Redemption (d)",
        "min_investment": "Min invest (BRL)",
    }
)

selection = st.dataframe(
    display.drop(columns=["fund_cnpj", "subclass_id"]),
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
)

selected_rows = selection.selection.rows if selection and selection.selection else []
if selected_rows:
    row = df.iloc[selected_rows[0]]
    st.session_state["selected_fund"] = {
        "fund_cnpj": row["fund_cnpj"],
        "subclass_id": (
            row["subclass_id"] if row["subclass_id"] == row["subclass_id"] else None
        ),
        "reference_date": ref_date,
        "purpose": purpose,
        "profile": profile,
        "investor_type": investor_type,
    }
    st.switch_page("pages/2_Fund.py")
else:
    st.caption("Click a row to open the fund detail page.")
