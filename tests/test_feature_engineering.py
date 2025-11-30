import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .getOrCreate()

def test_log_transformation(spark):
    """Test log transformation of value and gasPrice"""
    data = [(1000.0, 50.0), (5000.0, 100.0)]
    df = spark.createDataFrame(data, ["value", "gasPrice"])
    
    from pyspark.sql.functions import log1p
    df = df.withColumn("log_value", log1p("value"))
    df = df.withColumn("log_gasPrice", log1p("gasPrice"))
    
    result = df.collect()
    assert result[0]['log_value'] > 0
    assert result[0]['log_gasPrice'] > 0
    assert not df.filter(col("log_value").isNull()).count() > 0

def test_time_slot_creation(spark):
    """Test time_slot categorization"""
    from pyspark.sql.functions import hour, when
    
    data = [(3,), (9,), (15,), (21,)]  # hour values
    df = spark.createDataFrame(data, ["hour"])
    
    df = df.withColumn(
        "time_slot",
        when((col("hour") >= 0) & (col("hour") < 6), "midnight")
        .when((col("hour") >= 6) & (col("hour") < 12), "morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
        .otherwise("night")
    )
    
    result = df.collect()
    assert result[0]['time_slot'] == 'midnight'
    assert result[1]['time_slot'] == 'morning'
    assert result[2]['time_slot'] == 'afternoon'
    assert result[3]['time_slot'] == 'night'

def test_spike_detection_logic(spark):
    """Test anomaly spike detection with rolling window"""
    # Create sample data with one spike
    data = [
        ('0xabc', 100.0, 1),
        ('0xabc', 105.0, 2),
        ('0xabc', 102.0, 3),
        ('0xabc', 5000.0, 4),  # Spike
    ]
    df = spark.createDataFrame(data, ["from", "value", "timestamp"])
    
    # Simplified spike detection test
    from pyspark.sql.functions import log1p, mean, stddev
    from pyspark.sql.window import Window
    
    df = df.withColumn("log_value", log1p("value"))
    window = Window.partitionBy("from").orderBy("timestamp").rowsBetween(-2, -1)
    
    df = df.withColumn("rolling_mean", mean("log_value").over(window))
    df = df.withColumn("rolling_std", stddev("log_value").over(window))
    
    result = df.collect()
    # The spike should have much higher value than previous transactions
    assert result[3]['log_value'] > result[0]['log_value'] * 2

def test_contract_call_flag(spark):
    """Test is_contract_call flag creation"""
    from pyspark.sql.functions import when
    
    data = [('', 'address1'), ('0x456', 'address2')]
    df = spark.createDataFrame(data, ["contractAddress", "to"])
    
    df = df.withColumn(
        "is_contract_call",
        when(col("contractAddress") != "", 1).otherwise(0)
    )
    
    result = df.collect()
    assert result[0]['is_contract_call'] == 0
    assert result[1]['is_contract_call'] == 1
