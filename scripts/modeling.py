
# ## Modeling 


# ### 1. Install Packages 
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support


# ### 2. Feature Selection
feature_cols = [
    # log scale values
    "log_value",
    "log_gasPrice",
    # from transaction statistics
    "total_tx_count_from",
    "total_fail_rate_from",
    "daily_tx_count_from",
    "daily_fail_rate_from",
    "weekly_tx_count_from",
    "weekly_fail_rate_from",
    "daily_avg_interval_from",
    # to transaction statistics
    "total_tx_count_to",
    "total_fail_rate_to",
    "daily_tx_count_to",
    "daily_fail_rate_to",
    "weekly_tx_count_to",
    "weekly_fail_rate_to",
    "daily_avg_interval_to",
    # spike and zscore features
    "rolling_tx_count",
    "value_zscore",
    "gasPrice_zscore",
    "value_spike",
    "gasPrice_spike",
    # contract call
    "is_contract_call",
]

# pyspark DataFrame to pandas DataFrame
pdf = trx_df.select(
    "hash",
    "from",
    "to",
    "methodId",
    "functionName",
    "time_slot",
    *feature_cols  # Identifiers  # Feature columns
).toPandas()

# Pandas encoding categorical features
cat_cols = ["methodId", "functionName", "time_slot"]
pdf = pd.get_dummies(pdf, columns=cat_cols, dummy_na=True)
feature_cols = [c for c in pdf.columns if c not in ["hash", "from", "to"]]

X = pdf[feature_cols]


# ### 3. Model Training Setup

clf = IsolationForest(
    n_estimators=100,
    max_samples="auto",
    contamination=0.01,  # Expeting Fruad ratio is 1% among all transactions
    max_features=1.0,
    random_state=42,
    n_jobs=-1,
)


# ### 4. Model Training 

clf.fit(X)

# Prediction
# Prediction label: 1 for normal, -1 for anomaly
pdf["pred_label"] = clf.predict(X)
# Anomaly_score: the lower, the more abnormal
pdf["anomaly_score"] = clf.decision_function(X)

# Result interpretation
anomalies = pdf[pdf["pred_label"] == -1]
print(
    f"Number of transaction anomalies: {len(anomalies)} ({len(anomalies)/len(pdf)*100:.2f}%)"
)

# Top 10 suspicious transactions
top_anomalies = pdf.sort_values("anomaly_score").head(10)
print("\nTop 10 suspicious transactions:")
print(top_anomalies[["hash", "from", "to", "log_value", "pred_label", "anomaly_score"]])


