"""
Mortgage Portfolio Stability Index - Streamlit app
Federates FRED data (Snowflake) with BurstBank product profile via Starburst Galaxy.
Uses PyStarburst (Starburst's official Python client) per https://docs.starburst.io/clients/python/pystarburst.html
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
import trino.auth
from dotenv import load_dotenv
from pystarburst import Session

load_dotenv()

# Connection configuration from env - use exact values from Partner connect → PyStarburst tile
_raw_host = os.getenv("STARBURST_HOST", "").strip()
host = _raw_host.removeprefix("https://").removeprefix("http://").split("/")[0]
if host.endswith(".galaxy.starburst.io") and ".trino." not in host:
    host = host.replace(".galaxy.starburst.io", ".trino.galaxy.starburst.io")
username = os.getenv("STARBURST_USER", "").strip()
password = os.getenv("STARBURST_PASSWORD", "")


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL via PyStarburst Session and return a pandas DataFrame."""
    db_params = {
        "host": host,
        "port": 443,
        "http_scheme": "https",
        "user": username,
        "catalog": "sample",
        "schema": "burstbank",
        "auth": trino.auth.BasicAuthentication(username, password),
    }
    session = Session.builder.configs(db_params).create()
    try:
        return session.sql(query).to_pandas()
    finally:
        session.close()


# Page setup
st.set_page_config(page_title="Burstbank Stability Index", layout="wide")
st.title("🏦 Mortgage Portfolio Stability Index")
st.markdown("### Analyzing Loan Security vs. Market Volatility (Live Fed Rate: 6.22%)")

# Credentials check
if not all([host, username, password]):
    st.error(
        "Missing Starburst credentials. Set STARBURST_HOST, STARBURST_USER, and "
        "STARBURST_PASSWORD in your .env file or environment."
    )
    st.stop()

# Federated query
federated_query = """
WITH current_market_rate AS (
    SELECT value as val
    FROM fred.public.fred_observations
    WHERE series_id = 'MORTGAGE30US'
    ORDER BY date DESC
    LIMIT 1
)
SELECT
    p.mortgage_officer,
    AVG(p.mortgage_rate) as avg_cust_rate,
    AVG(m.val) as market_avg,
    AVG(p.mortgage_rate - m.val) as avg_rate_spread,
    COUNT(p.custkey) as total_customers
FROM
    sample.burstbank.product_profile p
CROSS JOIN
    current_market_rate m
WHERE
    p.mortgage_rate IS NOT NULL
GROUP BY
    p.mortgage_officer
ORDER BY
    avg_rate_spread ASC
"""

# Data execution
with st.spinner("Federating data across clouds..."):
    try:
        df = run_query(federated_query)
    except Exception as e:
        _hint = ""
        if "404" in str(e) and "Destination not found" in str(e):
            _hint = " **Tip:** In Starburst Galaxy → Partner connect, copy the Host from the **Trino Python** or **PyStarburst** tile for your cluster."
        st.error(f"Query failed: {e}{_hint}")
        st.stop()

if df.empty:
    st.warning("No data returned. Check catalog configuration (fred, sample.burstbank).")
    st.stop()

# Visualization
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Officer Performance: Market Spread")
    fig = px.bar(
        df,
        x="mortgage_officer",
        y="avg_rate_spread",
        color="avg_rate_spread",
        labels={"avg_rate_spread": "Stability Index (Negative is Safer)"},
        color_continuous_scale="RdYlGn_r",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Stability Leaderboard")
    leaderboard = (
        df[["mortgage_officer", "avg_rate_spread", "total_customers"]]
        .sort_values("avg_rate_spread")
    )
    st.dataframe(leaderboard, use_container_width=True)

# Strategic insight
top_officer = df.iloc[0]["mortgage_officer"]
top_spread = df.iloc[0]["avg_rate_spread"]
st.info(
    f"**Strategic Note:** Officer **{top_officer}** holds the most stable portfolio "
    f"with an average spread of **{top_spread:.2f}%** below current market rates."
)
