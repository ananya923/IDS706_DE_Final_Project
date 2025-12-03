
# ### 1. Install packages


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import pandas as pd


# ### 2. Data Transformation & Feature Engineering with PySpark


def process_transactions(spark: SparkSession, trx_results: list[dict]) -> DataFrame:
		"""
		Spark DataFrame with engineered features of Ethereum transactions.
		"""
    # 1. Create Spark DataFrame
    df = spark.createDataFrame(trx_results)
    
    # 2. Preprocessing
    # Check Schema Structure 
	  df.printSchema()
    
    # Transform 'contractAddress' column 
    df = df.withColumn("is_contract_call", when(col("contractAddress") != "", 1).otherwise(0))
    
    # Clean 'functionName' column data by removing () content 
    df = df.withColumn("functionName", split(col("functionName"), "\\(")[0])
    
    # Cast numeric columns
    numeric_cols = ['blockNumber', 'timeStamp', 'nonce', 'transactionIndex', 'value', 'gas', 'gasPrice',
                    'cumulativeGasUsed', 'gasUsed', 'confirmations', 'isError', 'txreceipt_status']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
            
    # Cast categorical columns
    cat_cols = ['from', 'to', 'methodId', 'functionName']
    for c in cat_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(StringType()))
    
    # 3. Feature engineering 
    # 3-1. Temporal Feature 
	  # timestamp -> datetime (UTC), date, week_start, hour 
	  df = df.withColumn("datetime", col("timeStamp").cast("timestamp"))
		df = df.withColumn("date", to_date(col("datetime")))
		df = df.withColumn("week_start", date_trunc("week", col("datetime")))
		df = df.withColumn("hour", hour(col("datetime")))
   
    # time-of-day slot
    df = df.withColumn("time_slot",
    when((col("hour") >= 0) & (col("hour") < 6), "midnight")
    .when((col("hour") >= 6) & (col("hour") < 12), "morning")
    .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
    .otherwise("night"))

    # transaction count and Fail rate 
    # Total 
    total_from = df.groupBy("from").agg(
        count("*").alias("total_tx_count_from"),
        sum(col("isError").cast("int")).alias("total_fail_count_from"))
    total_from = total_from.withColumn("total_fail_rate_from", (col("total_fail_count_from")+1)/(col("total_tx_count_from")+1)) #smoothing with +1
    total_to = df.groupBy("to").agg(
        count("*").alias("total_tx_count_to"),
        sum(col("isError").cast("int")).alias("total_fail_count_to"))
    total_to = total_to.withColumn("total_fail_rate_to", (col("total_fail_count_to")+1)/(col("total_tx_count_to")+1)) # smoothing with +1
    # Daily 
    daily_from = df.groupBy("from", "date").agg(
        count("*").alias("daily_tx_count_from"),
        sum(col("isError").cast("int")).alias("daily_fail_count_from"))
    daily_from = daily_from.withColumn("daily_fail_rate_from", (col("daily_fail_count_from")+1)/(col("daily_tx_count_from")+1)) 
    
    daily_to = df.groupBy("to", "date").agg(
        count("*").alias("daily_tx_count_to"),
        sum(col("isError").cast("int")).alias("daily_fail_count_to"))
    daily_to = daily_to.withColumn("daily_fail_rate_to", (col("daily_fail_count_to")+1)/(col("daily_tx_count_to")+1)) 
    # Weekly 
    weekly_from = df.groupBy("from", "week_start").agg(
        count("*").alias("weekly_tx_count_from"),
        sum(col("isError").cast("int")).alias("weekly_fail_count_from"))
    weekly_from = weekly_from.withColumn("weekly_fail_rate_from", (col("weekly_fail_count_from")+1)/(col("weekly_tx_count_from")+1))
	  weekly_to = df.groupBy("to", "week_start").agg(
        count("*").alias("weekly_tx_count_to"),
        sum(col("isError").cast("int")).alias("weekly_fail_count_to"))
    weekly_to = weekly_to.withColumn("weekly_fail_rate_to", (col("weekly_fail_count_to")+1)/(col("weekly_tx_count_to")+1))
    
    # Daily average transaction interval
		windowSpec_from = Window.partitionBy("from").orderBy("datetime")
		df = df.withColumn("prev_datetime_from", lag("datetime").over(windowSpec_from))
		df = df.withColumn("interval_from", 
		    (col("datetime").cast("long") - col("prev_datetime_from").cast("long")).cast(DoubleType()))

		windowSpec_to = Window.partitionBy("to").orderBy("datetime")
		df = df.withColumn("prev_datetime_to", lag("datetime").over(windowSpec_to))
		df = df.withColumn("interval_to", 
		    (col("datetime").cast("long") - col("prev_datetime_to").cast("long")).cast(DoubleType()))
		
		daily_avg_interval_from = (
    df.groupBy("from", "date")
      .agg(mean("interval_from").alias("daily_avg_interval_from")))
      
    daily_avg_interval_to = (
    df.groupBy("to", "date")
      .agg(mean("interval_to").alias("daily_avg_interval_to")))
    
    # 3-2. Value Feature 
    # Log transform 
    df = df.withColumn("log_value", log1p("value"))
    df = df.withColumn("log_gasPrice", log1p("gasPrice"))
    
    # Spike detection (3 stddev above rolling mean)
    rolling_window = Window.partitionBy("from").orderBy(col("datetime").cast("long")).rangeBetween(-7*86400, -1)

    df = df.withColumn("rolling_value_mean", mean("log_value").over(rolling_window))
    df = df.withColumn("rolling_value_std", stddev("log_value").over(rolling_window))
    df = df.withColumn("rolling_gasPrice_mean", mean("log_gasPrice").over(rolling_window))
    df = df.withColumn("rolling_gasPrice_std", stddev("log_gasPrice").over(rolling_window))
    df = df.withColumn("rolling_tx_count", count("*").over(rolling_window)) 
    
    df = df.withColumn("value_zscore",
    (col("log_value") - col("rolling_value_mean")) / col("rolling_value_std"))
		df = df.withColumn("gasPrice_zscore",
    (col("log_gasPrice") - col("rolling_gasPrice_mean")) / col("rolling_gasPrice_std"))
    
    df = df.withColumn("value_spike",
    when(col("rolling_tx_count") < 10, 0)  # Not enough data
    .when(col("rolling_value_std").isNull(), 0)  # No stddev
    .when(col("value_zscore") > 3, 1)  # Strong spike
    .otherwise(0))

		df = df.withColumn("gasPrice_spike",
		    when(col("rolling_tx_count") < 10, 0)
		    .when(col("rolling_gasPrice_std").isNull(), 0)
		    .when(col("gasPrice_zscore") > 3, 1)
		    .otherwise(0))
    
    # 4. Encoding 
    
    # Encoding coloums
		encoding_cols = ['methodId', 'functionName', 'time_slot']

		# StringIndexer (string -> number) 
		indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep")
    for col in encoding_cols]

		# OneHotEncoder (Number → one hot vector)
		encoders = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_onehot")
    for col in encoding_cols]

		# Pipeline execution 
		pipeline = Pipeline(stages=indexers + encoders)
		model = pipeline.fit(df)
		df_encoded = model.transform(df)
		
		# Results 
		df_encoded.select("methodId", "methodId_index", "methodId_onehot").show(5, truncate=False)
		
		# Join all features back to main df
    trx_df = df_encoded.join(daily_from, ["from","date"], "left")
    trx_df = df_encoded.join(daily_to, ["to","date"], "left")
    trx_df = df_encoded.join(weekly_from, ["from","week_start"], "left")
    trx_df = df_encoded.join(weekly_to, ["to","week_start"], "left")
    trx_df = df_encoded.join(total_from, ["from"], "left")
    trx_df = df_encoded.join(total_to, ["to"], "left")
    trx_df = df_encoded.join(daily_avg_interval_from, ["from","date"], "left")
    trx_df = df_encoded.join(daily_avg_interval_to, ["to","date"], "left")
    
    return trx_df


