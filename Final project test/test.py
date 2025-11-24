import numpy as np
import pandas as pd

pd.set_option("mode.copy_on_write", True)

files = [
    "/Users/shellyy/Desktop/IDS_706/Final project test/export-transaction-list-1763132085616.csv",
    "/Users/shellyy/Desktop/IDS_706/Final project test/export-transaction-list-1763132128380.csv",
    "/Users/shellyy/Desktop/IDS_706/Final project test/export-transaction-list-1763132177523.csv",
]

dfs = [pd.read_csv(path) for path in files]
df = pd.concat(dfs, ignore_index=True)

# Parse datetime
df["DateTime"] = pd.to_datetime(df["DateTime (UTC)"], utc=True)

# Clean ETH amount
df["eth_value"] = (
    df["Amount"]
    .str.replace(" ETH", "", regex=False)
    .astype(float)
)

# Clean USD value
df["usd_value"] = (
    df["Value (USD)"]
    .str.replace("[$,]", "", regex=True)
    .astype(float)
)

# Txn fee is already numeric ETH in your file
df["txn_fee_eth"] = df["Txn Fee"].astype(float)

# Normalize column names a bit
df = df.rename(
    columns={
        "Transaction Hash":"tx_hash",
        "Blockno": "block_number",
        "From": "from_address",
        "To": "to_address",
        "To_Nametag": "to_nametag",
        "From_Nametag": "from_nametag"
    }
)

# Keep the key columns
tx = df[
    [   'tx_hash',
        "Status",
        "Method",
        "block_number",
        "DateTime",
        "from_address",
        "to_address",
        "from_nametag",
        "to_nametag",
        "eth_value",
        "usd_value",
        "txn_fee_eth",
    ]
].copy()


# -------------------------------------------------------------------
# 2) LONG FORMAT: ONE ROW PER ADDRESS PER TRANSACTION
# -------------------------------------------------------------------

sent = tx.rename(
    columns={
        "from_address": "address",
        "to_address": "counterparty",
        "from_nametag": "address_nametag",
        "to_nametag": "counterparty_nametag",
    }
).copy()
sent["direction"] = "sent"

received = tx.rename(
    columns={
        "to_address": "address",
        "from_address": "counterparty",
        "to_nametag": "address_nametag",
        "from_nametag": "counterparty_nametag",
    }
).copy()
received["direction"] = "received"

# Combine
tx_long = pd.concat([sent, received], ignore_index=True)

tx_long = tx_long.dropna(subset=["address"])

tx_long["has_from_nametag"] = (
    (tx_long["direction"] == "sent") &
    tx_long["address_nametag"].notna()
).astype(int)

tx_long["has_to_nametag"] = (
    (tx_long["direction"] == "received") &
    tx_long["address_nametag"].notna()
).astype(int)



# -------------------------------------------------------------------
# 3) HELPER: AVERAGE MINUTES BETWEEN TXNS
# -------------------------------------------------------------------
def avg_minutes_between(datetime_series: pd.Series) -> float:
    """Average minutes between consecutive timestamps for one address."""
    s = datetime_series.sort_values()
    if s.size <= 1:
        return 0.0
    deltas = s.diff().dropna().dt.total_seconds() / 60.0
    return float(deltas.mean())

# -------------------------------------------------------------------
# 4) ADDRESS-LEVEL AGGREGATES
# -------------------------------------------------------------------

# Split sent / received views for convenience
sent_view = tx_long[tx_long["direction"] == "sent"].copy()
recv_view = tx_long[tx_long["direction"] == "received"].copy()

# ---- Counts & basic time window -----------------------------------
# Total distinct transactions an address ever appears in
total_tx = (
    tx_long.groupby("address")["tx_hash"]
    .nunique()
    .rename("total transactions (including tnx to create contract)")
)

# First–last time span (sec) for ANY direction
time_span = (
    tx_long.groupby("address")["DateTime"]
    .agg(
        lambda s: (s.max() - s.min()).total_seconds()
    )
    .rename("Time_Diff_first_and_last_(sec)")
)

#Sent side 
sent_counts = (
    sent_view.groupby("address")["tx_hash"]
    .nunique()
    .rename("Sent tnx")
)

sent_val_stats = sent_view.groupby("address")["eth_value"].agg(
    **{
        "min_val_sent": "min",
        "max_val_sent": "max",
        "avg_val_sent": "mean",
        "total_ether_sent": "sum",
    }
)

avg_min_between_sent = (
    sent_view.groupby("address")["DateTime"]
    .apply(avg_minutes_between)
    .rename("Avg min between sent tnx")
)

unique_sent_to = (
    sent_view.groupby("address")["counterparty"]
    .nunique()
    .rename("Unique Sent To Addresses")
)

# ---- Received side ------------------------------------------------
recv_counts = (
    recv_view.groupby("address")["tx_hash"]
    .nunique()
    .rename("Received_Tnx")
)

recv_val_stats = recv_view.groupby("address")["eth_value"].agg(
    **{
        "min_val_received": "min",
        "max_val_received": "max",
        "avg_val_received": "mean",
        "total_ether_received": "sum",
    }
)

avg_min_between_recv = (
    recv_view.groupby("address")["DateTime"]
    .apply(avg_minutes_between)
    .rename("Avg min between received tnx")
)

unique_recv_from = (
    recv_view.groupby("address")["counterparty"]
    .nunique()
    .rename("Unique Received From Addresses")
)

# ---- Combine everything -------------------------------------------
features = pd.concat(
    [
        total_tx,
        time_span,
        sent_counts,
        recv_counts,
        avg_min_between_sent,
        avg_min_between_recv,
        unique_recv_from,
        unique_sent_to,
        sent_val_stats,
        recv_val_stats,
    ],
    axis=1,
)

# Fill NaNs (e.g., addresses that only ever send or only ever receive)
features = features.fillna(0.0)

# Total ether balance (received - sent)
features["total_ether_balance"] = (
    features["total_ether_received"] - features["total_ether_sent"]
)


# tx has columns:
# "from_address", "to_address", "from_nametag", "to_nametag", ...

# 1) Flag for having a from_nametag at least once
has_from_nametag = (
    tx.groupby("from_address")["from_nametag"]
      .apply(lambda s: int(s.notna().any()))
      .rename("has_from_nametag")
)

# 2) Flag for having a to_nametag at least once
has_to_nametag = (
    tx.groupby("to_address")["to_nametag"]
      .apply(lambda s: int(s.notna().any()))
      .rename("has_to_nametag")
)

# 3) Combine them into one table, index = address
nametag_flags = pd.concat(
    [has_from_nametag, has_to_nametag],
    axis=1
)

# In case some addresses only appear in from or to, fill missing with 0
nametag_flags = nametag_flags.fillna(0).astype(int)
nametag_flags.index.name = "Address"

# 4) Merge into your per-address features table
features = features.join(nametag_flags, how="left")

# If any addresses in features never showed up in either from/to, set 0
features[["has_from_nametag", "has_to_nametag"]] = (
    features[["has_from_nametag", "has_to_nametag"]]
    .fillna(0)
    .astype(int)
)



#+++++++

# Kaggle-style placeholders (no contract creation or ERC20 info here)
features["Number of Created Contracts"] = 0.0
features["total ether sent contracts"] = 0.0

# ERC20 fields (set to 0; you don't have token-level data)
erc20_cols = [
    " Total ERC20 tnxs",
    " ERC20 total Ether received",
    " ERC20 total ether sent",
    " ERC20 total Ether sent contract",
    " ERC20 uniq sent addr",
    " ERC20 uniq rec addr",
    " ERC20 uniq sent addr.1",
    " ERC20 uniq rec contract addr",
    " ERC20 avg time between sent tnx",
    " ERC20 avg time between rec tnx",
    " ERC20 avg time between rec 2 tnx",
    " ERC20 avg time between contract tnx",
    " ERC20 min val rec",
    " ERC20 max val rec",
    " ERC20 avg val rec",
    " ERC20 min val sent",
    " ERC20 max val sent",
    " ERC20 avg val sent",
    " ERC20 min val sent contract",
    " ERC20 max val sent contract",
    " ERC20 avg val sent contract",
    " ERC20 uniq sent token name",
    " ERC20 uniq rec token name",
    " ERC20 most sent token type",
    " ERC20_most_rec_token_type",
]
for c in erc20_cols:
    features[c] = 0.0

# Add a placeholder fraud flag (all 0 for now)
features["FLAG"] = 0

# Make address the index name to match your second CSV style
features.index.name = "Address"

# OPTIONAL: save to CSV
#features.to_csv("transaction_like_features.csv")
