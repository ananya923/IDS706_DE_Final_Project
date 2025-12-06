import sys
import json
from datetime import datetime
import functools  # <-- use this for reduce

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

import boto3

# ==============================
# Glue job initialization
# ==============================

# Make OUTPUT_BUCKET optional with a default
base_args = getResolvedOptions(sys.argv, ["JOB_NAME"])
try:
    extra_args = getResolvedOptions(sys.argv, ["OUTPUT_BUCKET"])
    OUTPUT_BUCKET = extra_args["OUTPUT_BUCKET"]
except Exception:
    OUTPUT_BUCKET = "de-27-team11-new"
    print(f"⚠ OUTPUT_BUCKET not provided, defaulting to {OUTPUT_BUCKET}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(base_args["JOB_NAME"], base_args)


def load_features(glueContext, database_name: str, table_name: str):
    """
    Read processed features from Glue Catalog and return Spark DataFrame.
    """
    print(f"Reading processed features from {database_name}.{table_name}")

    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="features_source",
    )

    df = datasource.toDF()
    print(f"Total records loaded: {df.count()}")
    print("Schema:")
    df.printSchema()
    return df


def compute_anomaly_scores(df):
    """
    Pure PySpark anomaly scoring:
    - Select numeric feature columns
    - Compute z-scores for each feature
    - anomaly_score = sum of squared z-scores across features
    - Label top ~1% scores as anomalies (pred_label = -1)
    """
    feature_cols = [
        "log_value",
        "log_gasPrice",
        "total_tx_count_from",
        "total_fail_rate_from",
        "daily_tx_count_from",
        "daily_fail_rate_from",
        "weekly_tx_count_from",
        "weekly_fail_rate_from",
        "daily_avg_interval_from",
        "total_tx_count_to",
        "total_fail_rate_to",
        "daily_tx_count_to",
        "daily_fail_rate_to",
        "weekly_tx_count_to",
        "weekly_fail_rate_to",
        "daily_avg_interval_to",
        "rolling_tx_count",
        "value_zscore",
        "gasPrice_zscore",
        "value_spike",
        "gasPrice_spike",
        "is_contract_call",
    ]

    available_features = [c for c in feature_cols if c in df.columns]
    print(
        f"Using {len(available_features)}/{len(feature_cols)} feature columns for scoring."
    )

    # Cast to double
    for c in available_features:
        df = df.withColumn(c, col(c).cast(DoubleType()))

    # Compute mean and stddev for each feature
    agg_exprs = []
    for c in available_features:
        agg_exprs.append(avg(c).alias(f"{c}_mean"))
        agg_exprs.append(stddev(c).alias(f"{c}_std"))

    stats_row = df.agg(*agg_exprs).collect()[0].asDict()
    print("Feature stats (mean/std) computed.")

    # Create z-score columns
    z_cols = []
    for c in available_features:
        mean_val = stats_row.get(f"{c}_mean")
        std_val = stats_row.get(f"{c}_std")

        if std_val is None or std_val == 0:
            print(f"Skipping z-score for {c} (stddev is 0 or None).")
            continue

        z_col_name = f"{c}_z"
        df = df.withColumn(
            z_col_name,
            (col(c) - lit(mean_val)) / lit(std_val),
        )
        z_cols.append(z_col_name)

    print(f"Created {len(z_cols)} z-score columns.")

    # ----- SAFE ANOMALY SCORE COMPUTATION -----
    if len(z_cols) == 0:
        print("No z-score columns → anomaly_score = 0")
        df = df.withColumn("anomaly_score", lit(0.0))

    elif len(z_cols) == 1:
        print(f"Only 1 z-column ({z_cols[0]}) → anomaly_score = z^2")
        df = df.withColumn("anomaly_score", pow(col(z_cols[0]), 2))

    else:
        print(f"{len(z_cols)} z-columns → computing sum of squared z-scores")
        squared_terms = [pow(col(z), 2) for z in z_cols]
        # Use functools.reduce explicitly so we don't call pyspark.sql.functions.reduce
        anomaly_expr = functools.reduce(lambda a, b: a + b, squared_terms)
        df = df.withColumn("anomaly_score", anomaly_expr)

    print("Anomaly scores computed.")

    # 99th percentile threshold
    quantiles = df.approxQuantile("anomaly_score", [0.99], 0.01)
    threshold = quantiles[0] if quantiles and quantiles[0] is not None else None
    print(f"99th percentile threshold for anomaly_score: {threshold}")

    # Label anomalies: -1 for anomaly, 1 for normal
    if threshold is not None:
        df = df.withColumn(
            "pred_label",
            when(col("anomaly_score") >= lit(threshold), lit(-1)).otherwise(lit(1)),
        )
    else:
        df = df.withColumn("pred_label", lit(1))

    # Summary
    summary = df.groupBy("pred_label").agg(count("*").alias("count")).collect()
    print("Prediction label summary (pred_label -> count):")
    for row in summary:
        print(f"{row['pred_label']}: {row['count']}")

    return df, available_features


def save_predictions_parquet(df, bucket: str):
    """
    Save predictions to S3 in Parquet so a Glue crawler & Athena can query them.
    """
    predictions_prefix = "processed/ethereum/fraud_predictions/"

    print(
        f"Writing Parquet predictions to s3://{bucket}/{predictions_prefix} (partitioned by date if available)"
    )

    if "date" in df.columns:
        (
            df.write.mode("overwrite")
            .partitionBy("date")
            .parquet(f"s3://{bucket}/{predictions_prefix}")
        )
    else:
        df.write.mode("overwrite").parquet(
            f"s3://{bucket}/{predictions_prefix}"
        )

    print(
        f"Parquet predictions written to s3://{bucket}/{predictions_prefix}"
    )


def save_csv_and_metadata(df, feature_names, bucket: str):
    """
    Save CSV copies + basic metadata to S3 for inspection.
    """
    s3_client = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

    pdf = df.toPandas()

    # 1. Full predictions CSV
    predictions_path = f"models/predictions/predictions_{timestamp}.csv"
    csv_buffer = pdf.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket, Key=predictions_path, Body=csv_buffer.encode("utf-8")
    )
    print(f"Predictions (CSV) saved to s3://{bucket}/{predictions_path}")

    # 2. Anomalies CSV
    if "pred_label" in pdf.columns:
        anomalies = pdf[pdf["pred_label"] == -1]
    else:
        anomalies = pdf.iloc[0:0]

    anomalies_path = f"models/predictions/anomalies_{timestamp}.csv"
    anomalies_csv = anomalies.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket, Key=anomalies_path, Body=anomalies_csv.encode("utf-8")
    )
    print(f"Anomalies (CSV) saved to s3://{bucket}/{anomalies_path}")

    # 3. Feature metadata JSON
    feature_metadata = {
        "feature_names": feature_names,
        "num_features": len(feature_names),
        "model_type": "zscore_anomaly_scoring",
        "threshold_percentile": 0.99,
        "timestamp": timestamp,
    }

    metadata_path = f"models/isolation_forest/metadata_{timestamp}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_path,
        Body=json.dumps(feature_metadata, indent=2).encode("utf-8"),
    )
    print(f"Metadata saved to s3://{bucket}/{metadata_path}")


# ==============================
# Main execution
# ==============================
print("Starting modeling job (PySpark anomaly scoring)...")

df_features = load_features(
    glueContext=glueContext,
    database_name="ethereum_db",
    table_name="processed_txlist_features",
)

df_scored, feature_names_used = compute_anomaly_scores(df_features)

save_predictions_parquet(df_scored, bucket=OUTPUT_BUCKET)
save_csv_and_metadata(df_scored, feature_names_used, bucket=OUTPUT_BUCKET)

print("Modeling job completed successfully!")
job.commit()
