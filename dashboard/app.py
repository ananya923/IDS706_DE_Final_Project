import pandas as pd
from datetime import datetime
import streamlit as st

def load_mock_data():
    """Return a mock fraud dataset similar to the final RDS table."""
    data = [
        {
            "tx_hash": "0xaaa111",
            "from_address": "0xFAR001",
            "to_address": "0xEXC001",
            "value_eth": 1.25,
            "timestamp": "2025-11-26 12:34:56",
            "anomaly_score": 0.97,
            "fraud_flag": True,
        },
        {
            "tx_hash": "0xbbb222",
            "from_address": "0xFAR002",
            "to_address": "0xEXC002",
            "value_eth": 0.40,
            "timestamp": "2025-11-26 13:10:21",
            "anomaly_score": 0.88,
            "fraud_flag": True,
        },
        {
            "tx_hash": "0xccc333",
            "from_address": "0xLEG001",
            "to_address": "0xSHOP01",
            "value_eth": 0.05,
            "timestamp": "2025-11-26 14:02:03",
            "anomaly_score": 0.45,
            "fraud_flag": False,
        },
        {
            "tx_hash": "0xddd444",
            "from_address": "0xLEG002",
            "to_address": "0xSHOP02",
            "value_eth": 3.10,
            "timestamp": "2025-11-26 15:30:00",
            "anomaly_score": 0.75,
            "fraud_flag": False,
        },
        {
            "tx_hash": "0xeee555",
            "from_address": "0xBOT001",
            "to_address": "0xEXC003",
            "value_eth": 10.00,
            "timestamp": "2025-11-26 16:45:10",
            "anomaly_score": 0.99,
            "fraud_flag": True,
        },
    ]

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# Page configuration
st.set_page_config(
    page_title="Unusual Suspects - Fraud Dashboard",
    layout="wide"
)

# Main title
st.title("📊 Unusual Suspects - Ethereum Fraud Monitoring Dashboard")

st.write(
    """
    This is the initial version of the dashboard.
    It currently uses **mock data**, and will be connected to RDS and Glue outputs later.
    """
)

st.success("Streamlit is running successfully! 🎉")

# Load mock data
df = load_mock_data()

st.subheader("Mock fraud transactions")

# Sidebar filter for minimum anomaly score
min_score = st.sidebar.slider(
    "Minimum anomaly score",
    min_value=0.0,
    max_value=1.0,
    value=0.8,
    step=0.01,
)

# Filter data by anomaly score
filtered_df = df[df["anomaly_score"] >= min_score]

# Simple KPI cards
col1, col2, col3 = st.columns(3)
col1.metric("Total transactions", len(df))
col2.metric("Suspicious (filtered)", len(filtered_df))
col3.metric("Threshold", f"{min_score:.2f}")

# Show the filtered table
st.dataframe(filtered_df, use_container_width=True)

# Fraud risk summary KPIs
st.subheader("Fraud risk summary")

total_tx = len(df)
fraud_tx = int(df["fraud_flag"].sum())
fraud_rate = fraud_tx / total_tx if total_tx > 0 else 0.0
avg_score = float(df["anomaly_score"].mean())
max_score = float(df["anomaly_score"].max())
unique_suspicious = df.loc[df["fraud_flag"], "from_address"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Fraudulent transactions", fraud_tx)
k2.metric("Fraud rate", f"{fraud_rate:.0%}")
k3.metric("Avg anomaly score", f"{avg_score:.2f}")
k4.metric("Suspicious source wallets", int(unique_suspicious))

st.subheader("Daily fraud trend")

daily = (
    df.set_index("timestamp")
      .resample("D")
      .agg(
          total_tx=("tx_hash", "count"),
          fraud_tx=("fraud_flag", "sum"),
      )
      .reset_index()
)

# Use timestamp as index for plotting
trend_data = daily.set_index("timestamp")[["total_tx", "fraud_tx"]]
st.line_chart(trend_data)

st.subheader("Top suspicious addresses")

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


st.subheader("Anomaly score distribution")

# Use all transactions for the distribution (not only filtered ones)
score_counts = df["anomaly_score"].value_counts().sort_index()
st.bar_chart(score_counts)
