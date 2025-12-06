import os
import pandas as pd
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine


# ------------------------------
# LOAD REAL DATA FROM RDS
# ------------------------------
def load_real_data():
    """
    Load fraud transactions from RDS final table.
    Expected columns:
      tx_hash, from_address, to_address, value_eth,
      timestamp, anomaly_score, fraud_flag
    """

    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT", "3306")  # MySQL default
    DB_NAME = os.environ.get("DB_NAME")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")

    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError("❌ Missing DB environment variables.")

    # MySQL engine (no hard-coded secrets)
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    query = """
        SELECT
            tx_hash,
            from_address,
            to_address,
            value_eth,
            timestamp,
            anomaly_score,
            fraud_flag
        FROM fraud_scored_tx
        ORDER BY timestamp;
    """

    df = pd.read_sql(query, engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_data():
    """
    Wrapper used by tests – simply returns real RDS data.
    """
    return load_real_data()


# ------------------------------
# STREAMLIT SETUP
# ------------------------------
st.set_page_config(
    page_title="Unusual Suspects - Fraud Dashboard",
    layout="wide",
)

st.title("📊 Unusual Suspects - Ethereum Fraud Monitoring Dashboard")
st.success("Connected to **REAL RDS DATA** 🎉")

# Load data from RDS
try:
    df = load_data()
except Exception as e:
    st.error(f"Error loading RDS data: {e}")
    st.stop()

# ------------------------------
# Dashboard Content
# ------------------------------
st.subheader("Fraud Transactions (Live from RDS)")

# Sidebar filter
min_score = st.sidebar.slider(
    "Minimum anomaly score",
    min_value=0.0,
    max_value=1.0,
    value=0.8,
    step=0.01,
)

filtered_df = df[df["anomaly_score"] >= min_score]

# KPI CARDS
col1, col2, col3 = st.columns(3)
col1.metric("Total transactions", len(df))
col2.metric("Suspicious (filtered)", len(filtered_df))
col3.metric("Threshold", f"{min_score:.2f}")

# Show filtered table
st.dataframe(filtered_df, use_container_width=True)

# Fraud summary
st.subheader("Fraud Risk Summary")

total_tx = len(df)
fraud_tx = int(df["fraud_flag"].sum())
fraud_rate = fraud_tx / total_tx if total_tx > 0 else 0
avg_score = float(df["anomaly_score"].mean())
unique_suspicious = df.loc[df["fraud_flag"], "from_address"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Fraudulent transactions", fraud_tx)
k2.metric("Fraud rate", f"{fraud_rate:.0%}")
k3.metric("Avg anomaly score", f"{avg_score:.2f}")
k4.metric("Suspicious source wallets", int(unique_suspicious))

# Trend
st.subheader("Daily Fraud Trend")

daily = (
    df.set_index("timestamp")
    .resample("D")
    .agg(
        total_tx=("tx_hash", "count"),
        fraud_tx=("fraud_flag", "sum"),
    )
    .reset_index()
)

trend_data = daily.set_index("timestamp")[["total_tx", "fraud_tx"]]
st.line_chart(trend_data)

# Top wallets
st.subheader("Top Suspicious Addresses")

col_left, col_right = st.columns(2)

with col_left:
    st.caption("Top source addresses (from_address)")
    top_from = (
        df[df["fraud_flag"]]
        .groupby("from_address")
        .size()
        .reset_index(name="fraud_count")
        .sort_values("fraud_count", ascending=False)
        .head(5)
    )
    st.dataframe(top_from, use_container_width=True)

with col_right:
    st.caption("Top destination addresses (to_address)")
    top_to = (
        df[df["fraud_flag"]]
        .groupby("to_address")
        .size()
        .reset_index(name="fraud_count")
        .sort_values("fraud_count", ascending=False)
        .head(5)
    )
    st.dataframe(top_to, use_container_width=True)

# Score distribution
st.subheader("Anomaly Score Distribution")

score_counts = df["anomaly_score"].value_counts().sort_index()
st.bar_chart(score_counts)
