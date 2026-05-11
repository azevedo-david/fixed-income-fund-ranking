"""Streamlit entry point. Run with: streamlit run dashboard/app.py"""

import streamlit as st

st.set_page_config(
    page_title="Fixed Income Fund Ranking",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

rankings_page = st.Page("pages/1_Rankings.py", title="Rankings", default=True)
fund_page = st.Page("pages/2_Fund.py", title="Fund detail")
validation_page = st.Page("pages/3_Validation.py", title="Validation")

nav = st.navigation([rankings_page, fund_page, validation_page])
nav.run()
