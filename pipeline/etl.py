import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp

def build_silver_layer():
    print("Initializing Sentinel Spark Engine...")
    
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Sentinel_ETL_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # 2. Define the Strict Financial Schema
    # This prevents bad/corrupt data from crashing the ML model later
    trade_schema = StructType([
        StructField("trade_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("trader_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("order_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("label", IntegerType(), True) # 0=Normal, 1-5=Anomalies
    ])

    raw_data_path = "data/raw/advanced_trades_dataset.json"
    silver_data_path = "data/silver_trades/"

    print(f"Reading raw JSON data from: {raw_data_path}")

    # 3. Ingest the Data
    df = spark.read.schema(trade_schema).json(raw_data_path)

    # 4. Data Cleansing & Transformation
    # Drop any trades that somehow don't have a Trade ID or Symbol
    df_cleaned = df.dropna(subset=["trade_id", "symbol"])
    
    # Add a processing timestamp for auditing
    df_transformed = df_cleaned.withColumn("ingestion_timestamp", current_timestamp())

    print("Data ingested and transformed. Writing to Silver Data Lake as Parquet...")

    # 5. Write to Parquet (Partitioned by Symbol)
    df_transformed.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(silver_data_path)

    print(f"Success! Silver layer built at: {silver_data_path}")
    
    # Show a preview of the clean data
    df_transformed.show(5)
    
    spark.stop()

if __name__ == "__main__":
    # Ensure the silver directory exists
    os.makedirs('data/silver_trades', exist_ok=True)
    build_silver_layer()