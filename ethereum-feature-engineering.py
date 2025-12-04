import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

# ===============================
# Init Glue job
# ===============================
required_args = ["JOB_NAME", "OUTPUT_BUCKET"]
args = getResolvedOptions(sys.argv, required_args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

OUTPUT_BUCKET = args["OUTPUT_BUCKET"]


def clean_column_names(df):
    """
    Ensure no column name is empty/whitespace.
    If a name is blank, rename it to 'col_<index>'.

    This avoids 'Cannot have an empty string for name' errors
    from Spark when creating/writing DataFrames.
    """
    safe_names = []
    for idx, c in enumerate(df.columns):
        if c is None:
            c = ""
        name = c.strip()
        if name == "":
            new_name = f"col_{idx}"
            print(
                f"⚠ Found blank column name at position {idx}, renaming to '{new_name}'"
            )
            safe_names.append(new_name)
        else:
            safe_names.append(name)
    df = df.toDF(*safe_names)
    return df


def process_transactions(glueContext, database_name: str, table_name: str):
    """
    Read raw Ethereum tx JSON from Glue Catalog, flatten 'result',
    engineer features, and return a Spark DataFrame.
    """
    print(f"Reading data from {database_name}.{table_name}")
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, table_name=table_name, transformation_ctx="datasource"
    )
    df = datasource.toDF()

    print("Initial schema from catalog:")
    df.printSchema()
    print(f"Initial row count: {df.count()}")

    # ---------------------------
    # 1. Flatten JSON structure
    # ---------------------------
    # Raw JSON looks like:
    # {
    #   "status": "1",
    #   "message": "OK",
    #   "result": [ {blockNumber, timeStamp, from, to, ...}, ... ]
    # }
    if "result" in df.columns:
        print("Found 'result' column, flattening array of transactions...")
        df = df.select(explode(col("result")).alias("tx"))
        df = df.select("tx.*")
    else:
        print("⚠ No 'result' column found; assuming table is already flat")

    print("Schema after flattening result (if any):")
    df.printSchema()
    print(f"Row count after flatten: {df.count()}")

    # Clean any weird/blank column names right after flatten
    df = clean_column_names(df)
    print("Schema after cleaning column names:")
    df.printSchema()

    # ---------------------------
    # 2. Preprocessing
    # ---------------------------
    print("Step 1: Preprocessing...")

    # Expected columns from your JSON
    required_cols = [
        "blockNumber",
        "timeStamp",
        "hash",
        "nonce",
        "blockHash",
        "transactionIndex",
        "from",
        "to",
        "value",
        "gas",
        "gasPrice",
        "isError",
        "txreceipt_status",
        "input",
        "contractAddress",
        "cumulativeGasUsed",
        "gasUsed",
        "confirmations",
        "methodId",
        "functionName",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠ WARNING: Missing expected columns: {missing}")

    # is_contract_call: contractAddress != ""
    if "contractAddress" in df.columns:
        df = df.withColumn(
            "is_contract_call", when(col("contractAddress") != "", 1).otherwise(0)
        )
    else:
        df = df.withColumn("is_contract_call", lit(0))

    # Clean functionName: drop '(...)' suffix
    if "functionName" in df.columns:
        df = df.withColumn("functionName", split(col("functionName"), "\\(")[0])

    # Cast numeric columns
    numeric_cols = [
        "blockNumber",
        "timeStamp",
        "nonce",
        "transactionIndex",
        "value",
        "gas",
        "gasPrice",
        "cumulativeGasUsed",
        "gasUsed",
        "confirmations",
        "isError",
        "txreceipt_status",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))

    # Cast categorical columns
    cat_cols = ["from", "to", "methodId", "functionName"]
    for c in cat_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(StringType()))

    # ---------------------------
    # 3. Temporal features
    # ---------------------------
    print("Step 2: Creating temporal features...")

    if "timeStamp" in df.columns:
        df = df.withColumn("datetime", col("timeStamp").cast("timestamp"))
    else:
        print("⚠ No timeStamp column found; creating null datetime")
        df = df.withColumn("datetime", lit(None).cast("timestamp"))

    df = df.withColumn("date", to_date(col("datetime")))
    df = df.withColumn("week_start", date_trunc("week", col("datetime")))
    df = df.withColumn("hour", hour(col("datetime")))

    df = df.withColumn(
        "time_slot",
        when((col("hour") >= 0) & (col("hour") < 6), "midnight")
        .when((col("hour") >= 6) & (col("hour") < 12), "morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
        .otherwise("night"),
    )

    # ---------------------------
    # 4. Aggregated stats
    # ---------------------------
    print("Step 3: Computing aggregated statistics...")

    # Total stats by from/to
    total_from = df.groupBy("from").agg(
        count("*").alias("total_tx_count_from"),
        sum(col("isError").cast("int")).alias("total_fail_count_from"),
    )
    total_from = total_from.withColumn(
        "total_fail_rate_from",
        (col("total_fail_count_from") + 1) / (col("total_tx_count_from") + 1),
    )

    total_to = df.groupBy("to").agg(
        count("*").alias("total_tx_count_to"),
        sum(col("isError").cast("int")).alias("total_fail_count_to"),
    )
    total_to = total_to.withColumn(
        "total_fail_rate_to",
        (col("total_fail_count_to") + 1) / (col("total_tx_count_to") + 1),
    )

    # Daily stats
    daily_from = df.groupBy("from", "date").agg(
        count("*").alias("daily_tx_count_from"),
        sum(col("isError").cast("int")).alias("daily_fail_count_from"),
    )
    daily_from = daily_from.withColumn(
        "daily_fail_rate_from",
        (col("daily_fail_count_from") + 1) / (col("daily_tx_count_from") + 1),
    )

    daily_to = df.groupBy("to", "date").agg(
        count("*").alias("daily_tx_count_to"),
        sum(col("isError").cast("int")).alias("daily_fail_count_to"),
    )
    daily_to = daily_to.withColumn(
        "daily_fail_rate_to",
        (col("daily_fail_count_to") + 1) / (col("daily_tx_count_to") + 1),
    )

    # Weekly stats
    weekly_from = df.groupBy("from", "week_start").agg(
        count("*").alias("weekly_tx_count_from"),
        sum(col("isError").cast("int")).alias("weekly_fail_count_from"),
    )
    weekly_from = weekly_from.withColumn(
        "weekly_fail_rate_from",
        (col("weekly_fail_count_from") + 1) / (col("weekly_tx_count_from") + 1),
    )

    weekly_to = df.groupBy("to", "week_start").agg(
        count("*").alias("weekly_tx_count_to"),
        sum(col("isError").cast("int")).alias("weekly_fail_count_to"),
    )
    weekly_to = weekly_to.withColumn(
        "weekly_fail_rate_to",
        (col("weekly_fail_count_to") + 1) / (col("weekly_tx_count_to") + 1),
    )

    # ---------------------------
    # 5. Transaction intervals
    # ---------------------------
    print("Step 4: Computing transaction intervals...")

    window_from = Window.partitionBy("from").orderBy("datetime")
    window_to = Window.partitionBy("to").orderBy("datetime")

    df = df.withColumn("prev_datetime_from", lag("datetime").over(window_from))
    df = df.withColumn(
        "interval_from",
        (col("datetime").cast("long") - col("prev_datetime_from").cast("long")).cast(
            DoubleType()
        ),
    )

    df = df.withColumn("prev_datetime_to", lag("datetime").over(window_to))
    df = df.withColumn(
        "interval_to",
        (col("datetime").cast("long") - col("prev_datetime_to").cast("long")).cast(
            DoubleType()
        ),
    )

    daily_avg_interval_from = df.groupBy("from", "date").agg(
        mean("interval_from").alias("daily_avg_interval_from")
    )
    daily_avg_interval_to = df.groupBy("to", "date").agg(
        mean("interval_to").alias("daily_avg_interval_to")
    )

    # ---------------------------
    # 6. Value features
    # ---------------------------
    print("Step 5: Computing value-related features...")

    df = df.withColumn("log_value", log1p(col("value")))
    df = df.withColumn("log_gasPrice", log1p(col("gasPrice")))

    rolling_window = (
        Window.partitionBy("from")
        .orderBy(col("datetime").cast("long"))
        .rangeBetween(-7 * 86400, -1)
    )

    df = df.withColumn("rolling_value_mean", mean("log_value").over(rolling_window))
    df = df.withColumn("rolling_value_std", stddev("log_value").over(rolling_window))
    df = df.withColumn(
        "rolling_gasPrice_mean", mean("log_gasPrice").over(rolling_window)
    )
    df = df.withColumn(
        "rolling_gasPrice_std", stddev("log_gasPrice").over(rolling_window)
    )
    df = df.withColumn("rolling_tx_count", count("*").over(rolling_window))

    df = df.withColumn(
        "value_zscore",
        (col("log_value") - col("rolling_value_mean")) / col("rolling_value_std"),
    )
    df = df.withColumn(
        "gasPrice_zscore",
        (col("log_gasPrice") - col("rolling_gasPrice_mean"))
        / col("rolling_gasPrice_std"),
    )

    df = df.withColumn(
        "value_spike",
        when(col("rolling_tx_count") < 10, 0)
        .when(col("rolling_value_std").isNull(), 0)
        .when(col("value_zscore") > 3, 1)
        .otherwise(0),
    )

    df = df.withColumn(
        "gasPrice_spike",
        when(col("rolling_tx_count") < 10, 0)
        .when(col("rolling_gasPrice_std").isNull(), 0)
        .when(col("gasPrice_zscore") > 3, 1)
        .otherwise(0),
    )

    # ---------------------------
    # 7. (TEMP) Skip encoding in Glue
    # ---------------------------
    print(
        "Step 6: Skipping StringIndexer/OneHotEncoder in Glue to avoid Spark ML naming bug."
    )
    print("       methodId, functionName, time_slot remain as string columns for now.")

    df_encoded = df  # no additional columns yet

    # ---------------------------
    # 8. Join all feature tables
    # ---------------------------
    print("Step 7: Joining aggregated features...")

    trx_df = df_encoded
    trx_df = trx_df.join(daily_from, ["from", "date"], "left")
    trx_df = trx_df.join(daily_to, ["to", "date"], "left")
    trx_df = trx_df.join(weekly_from, ["from", "week_start"], "left")
    trx_df = trx_df.join(weekly_to, ["to", "week_start"], "left")
    trx_df = trx_df.join(total_from, ["from"], "left")
    trx_df = trx_df.join(total_to, ["to"], "left")
    trx_df = trx_df.join(daily_avg_interval_from, ["from", "date"], "left")
    trx_df = trx_df.join(daily_avg_interval_to, ["to", "date"], "left")

    # Final safety: ensure no blank column names after joins
    trx_df = clean_column_names(trx_df)

    print(f"Final record count: {trx_df.count()}")
    print(f"Final column count: {len(trx_df.columns)}")

    return trx_df


# ===============================
# Main job
# ===============================
print("=" * 60)
print("Starting Ethereum ETL job...")
print("=" * 60)

trx_df = process_transactions(
    glueContext=glueContext,
    database_name="ethereum_db",
    table_name="raw_txlist",  # <-- your Glue table
)

output_s3_path = f"s3://{OUTPUT_BUCKET}/processed/ethereum/txlist_features/"
print(f"\n{'=' * 60}")
print(f"Writing output to S3: {output_s3_path}")
print(f"{'=' * 60}")

trx_df.write.mode("overwrite").partitionBy("date").parquet(output_s3_path)

print("✓ S3 output complete!")
print("\n============================================================")
print("ETL job completed successfully!")
print("============================================================")

job.commit()
