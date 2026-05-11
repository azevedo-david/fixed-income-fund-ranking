"""Pipeline validation page: per-task check results from logs.validation_log."""

import streamlit as st

import data

st.title("Pipeline Validation")

ref_dates = data.validation_reference_dates()
if not ref_dates:
    st.warning("No validation logs found.")
    st.stop()

with st.sidebar:
    st.header("Filters")
    ref_date = st.selectbox(
        "Reference date",
        ref_dates,
        format_func=lambda d: d.strftime("%Y-%m-%d"),
    )
    logged_dates = data.validation_logged_dates(ref_date)
    logged_date = st.selectbox(
        "Logged date (optional)",
        [None] + logged_dates,
        format_func=lambda d: d.strftime("%Y-%m-%d") if d else "All",
    )
    show_passing = st.checkbox("Show passing checks", value=False)

# ── summary strip ─────────────────────────────────────────────────────────────
summary = data.validation_summary(ref_date, logged_date)

if summary.empty:
    st.info("No checks logged for this reference date.")
    st.stop()

TASK_ORDER = ["validate_ingestion", "validate_staging", "validate_marts"]
summary["_sort"] = summary["task"].map(
    lambda t: TASK_ORDER.index(t) if t in TASK_ORDER else 99
)
summary = summary.sort_values("_sort").drop(columns="_sort")

cols = st.columns(len(summary))
for col, (_, row) in zip(cols, summary.iterrows()):
    label = row["task"].replace("validate_", "").capitalize()
    errors = int(row["errors"])
    warnings = int(row["warnings"])
    total = int(row["total"])
    passed = int(row["passed"])

    if errors > 0:
        icon, color = "🔴", "red"
        status = f"{errors} error{'s' if errors > 1 else ''}"
    elif warnings > 0:
        icon, color = "🟡", "orange"
        status = f"{warnings} warning{'s' if warnings > 1 else ''}"
    else:
        icon, color = "🟢", "green"
        status = "all pass"

    col.metric(
        label=f"{icon} {label}",
        value=f"{passed}/{total}",
        delta=status,
        delta_color="off" if errors == 0 and warnings == 0 else "inverse",
    )
    ts = row["last_logged_at"]
    col.caption(f"dataset: `{row['dataset']}` · logged {str(ts)[:19]}")

st.divider()

# ── per-task detail ───────────────────────────────────────────────────────────
for _, row in summary.iterrows():
    task = row["task"]
    label = task.replace("validate_", "").capitalize()
    errors = int(row["errors"])
    warnings = int(row["warnings"])

    if errors > 0:
        header_icon = "🔴"
    elif warnings > 0:
        header_icon = "🟡"
    else:
        header_icon = "🟢"

    # Auto-expand sections that have failures
    with st.expander(
        f"{header_icon} {label} — {row['dataset']}",
        expanded=(errors + warnings) > 0,
    ):
        detail = data.validation_detail(ref_date, task, logged_date)

        if not show_passing:
            filtered = detail[~detail["passed"]]
        else:
            filtered = detail

        if filtered.empty:
            st.success("All checks passed.")
            continue

        for _, chk in filtered.iterrows():
            passed = chk["passed"]
            severity = chk["severity"]
            name = chk["check_name"]
            value = chk["value"] if chk["value"] is not None else "—"
            threshold = chk["threshold"] if chk["threshold"] is not None else "—"
            message = chk["message"] if chk["message"] is not None else ""

            if passed:
                row_icon = "✅"
            elif severity == "error":
                row_icon = "❌"
            else:
                row_icon = "⚠️"

            c1, c2, c3, c4 = st.columns([3, 1, 2, 2])
            c1.markdown(f"{row_icon} `{name}`")
            c2.markdown(f"`{severity}`")
            c3.markdown(f"value: `{value}`")
            c4.markdown(f"threshold: `{threshold}`")
            if message:
                st.caption(f"↳ {message}")
