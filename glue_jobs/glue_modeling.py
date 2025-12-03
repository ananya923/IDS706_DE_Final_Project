import sys
import json
import pickle
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME", "OUTPUT_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Job parameters
OUTPUT_BUCKET = args["OUTPUT_BUCKET"]


def train_isolation_forest_model(glueContext, database_name: str, table_name: str):
    """
    Read processed features from Glue Catalog and train Isolation Forest model.
    """
    print(f"Reading processed features from {database_name}.{table_name}")

    # 1. Read data from Glue Data Catalog
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, table_name=table_name, transformation_ctx="datasource"
    )

    # Convert to Spark DataFrame, then to Pandas
    trx_df = datasource.toDF()
    print(f"Total records: {trx_df.count()}")

    # 2. Feature Selection
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

    # Select identifier columns + features
    identifier_cols = ["hash", "from", "to", "methodId", "functionName", "time_slot"]

    # Check which columns exist in the dataframe
    available_id_cols = [col for col in identifier_cols if col in trx_df.columns]
    available_feature_cols = [col for col in feature_cols if col in trx_df.columns]

    print(
        f"Available feature columns: {len(available_feature_cols)}/{len(feature_cols)}"
    )

    # Select and convert to Pandas
    pdf = trx_df.select(*(available_id_cols + available_feature_cols)).toPandas()

    # Handle categorical encoding for methodId, functionName, time_slot
    cat_cols = ["methodId", "functionName", "time_slot"]
    existing_cat_cols = [col for col in cat_cols if col in pdf.columns]

    if existing_cat_cols:
        print(f"Encoding categorical features: {existing_cat_cols}")
        pdf = pd.get_dummies(pdf, columns=existing_cat_cols, dummy_na=True)

    # Update feature columns list (after one-hot encoding)
    feature_cols_final = [c for c in pdf.columns if c not in available_id_cols]
    print(f"Final feature count after encoding: {len(feature_cols_final)}")

    X = pdf[feature_cols_final]

    # Handle missing values
    X = X.fillna(0)

    print(f"Feature matrix shape: {X.shape}")

    # 3. Model Training Setup
    print("Training Isolation Forest model...")
    clf = IsolationForest(
        n_estimators=100,
        max_samples="auto",
        contamination=0.01,  # Expecting Fraud ratio is 1% among all transactions
        max_features=1.0,
        random_state=42,
        n_jobs=-1,
    )

    # 4. Model Training
    clf.fit(X)

    # 5. Prediction
    # Prediction label: 1 for normal, -1 for anomaly
    pdf["pred_label"] = clf.predict(X)
    # Anomaly_score: the lower, the more abnormal
    pdf["anomaly_score"] = clf.decision_function(X)

    # 6. Result interpretation
    anomalies = pdf[pdf["pred_label"] == -1]
    print(
        f"\nNumber of transaction anomalies: {len(anomalies)} ({len(anomalies)/len(pdf)*100:.2f}%)"
    )

    # Top 10 suspicious transactions
    top_anomalies = pdf.sort_values("anomaly_score").head(10)
    print("\nTop 10 suspicious transactions:")
    display_cols = [
        col
        for col in ["hash", "from", "to", "log_value", "pred_label", "anomaly_score"]
        if col in top_anomalies.columns
    ]
    print(top_anomalies[display_cols])

    # 7. Save results
    return pdf, clf, feature_cols_final


def save_results_to_s3(
    predictions_df: pd.DataFrame, model, feature_names: list, bucket: str
):
    """
    Save model predictions and trained model to S3.
    """
    s3_client = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

    # 1. Save predictions as CSV
    predictions_path = f"models/predictions/predictions_{timestamp}.csv"
    csv_buffer = predictions_df.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket, Key=predictions_path, Body=csv_buffer.encode("utf-8")
    )
    print(f"Predictions saved to s3://{bucket}/{predictions_path}")

    # 2. Save model as pickle
    model_path = f"models/isolation_forest/model_{timestamp}.pkl"
    model_buffer = pickle.dumps(model)
    s3_client.put_object(Bucket=bucket, Key=model_path, Body=model_buffer)
    print(f"Model saved to s3://{bucket}/{model_path}")

    # 3. Save feature names for reference
    feature_metadata = {
        "feature_names": feature_names,
        "num_features": len(feature_names),
        "model_timestamp": timestamp,
        "contamination": 0.01,
        "n_estimators": 100,
    }
    metadata_path = f"models/isolation_forest/metadata_{timestamp}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_path,
        Body=json.dumps(feature_metadata, indent=2).encode("utf-8"),
    )
    print(f"Metadata saved to s3://{bucket}/{metadata_path}")

    # 4. Save anomalies separately for easy access
    anomalies = predictions_df[predictions_df["pred_label"] == -1]
    anomalies_path = f"models/predictions/anomalies_{timestamp}.csv"
    anomalies_csv = anomalies.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket, Key=anomalies_path, Body=anomalies_csv.encode("utf-8")
    )
    print(f"Anomalies saved to s3://{bucket}/{anomalies_path}")


# Main execution
print("Starting modeling job...")

# Train model
predictions_df, trained_model, feature_names = train_isolation_forest_model(
    glueContext=glueContext,
    database_name="ethereum_db",
    table_name="processed_txlist_features",
)

# Save results to S3
save_results_to_s3(
    predictions_df=predictions_df,
    model=trained_model,
    feature_names=feature_names,
    bucket=OUTPUT_BUCKET,
)

print("Modeling job completed successfully!")

job.commit()
