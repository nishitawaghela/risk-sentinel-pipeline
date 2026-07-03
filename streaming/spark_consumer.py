import os
import time  # <-- Required for latency calculation
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import shap
# 1. Load our trained Random Forest Model
MODEL_PATH = '/Users/nishitawaghela/sentinel/ml_engine/saved_models/anomaly_detector_v2.pkl'
print("Loading ML Model...")
model = joblib.load(MODEL_PATH)

# Define the exact features our model expects
EXPECTED_FEATURES = ['price', 'volume', 'action_SELL', 'order_type_MARKET']

# 2. Initialize Spark Session with Kafka Support
spark = SparkSession.builder \
    .appName("SentinelRiskEngine") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Define the Schema of our incoming Kafka JSON
# WE ADDED THE INGESTION TIMESTAMP HERE
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("stock_symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("ingestion_timestamp", DoubleType(), True) # <-- The timestamp from FastAPI
])

# 4. The Micro-Batch Processing Function
def process_batch(df, epoch_id):
    # Convert the Spark DataFrame to a Pandas DataFrame for the ML model
    pdf = df.toPandas()
    
    if pdf.empty:
        return
    
    # STOP THE CLOCK for this batch
    current_time = time.time()
    
    print(f"\n--- Processing Batch {epoch_id} | {len(pdf)} trades ---")
    
    # Feature Engineering
    # Safely handle missing columns if testing with old JSON formats
    if 'action' in pdf.columns:
        pdf['action_SELL'] = (pdf['action'] == 'SELL').astype(int)
    else:
        pdf['action_SELL'] = 0
        
    if 'order_type' in pdf.columns:
        pdf['order_type_MARKET'] = (pdf['order_type'] == 'MARKET').astype(int)
    else:
        pdf['order_type_MARKET'] = 0
    
    X = pdf[EXPECTED_FEATURES]
    
    # Make Predictions
    predictions = model.predict(X)
    pdf['risk_prediction'] = predictions
    
    # Calculate Latency for every trade in the batch
    if 'ingestion_timestamp' in pdf.columns:
        pdf['latency_ms'] = (current_time - pdf['ingestion_timestamp']) * 1000
    else:
        pdf['latency_ms'] = 0.0
    
    # Separate anomalies and normal trades
    anomalies = pdf[pdf['risk_prediction'] != 0] 
    clean_trades = pdf[pdf['risk_prediction'] == 0]
    
    # Print Alerts with Latency
    if not anomalies.empty:
        for index, row in anomalies.iterrows():
            print(f"🚨 FRAUD FLAGGED | Trade: {row.get('trade_id', 'N/A')} | End-to-End Latency: {row['latency_ms']:.2f} ms")
            
    if not clean_trades.empty:
        # We only print the first 5 clean trades per batch so the terminal doesn't crash from printing too fast
        for index, row in clean_trades.head(5).iterrows():
            print(f"✅ CLEAN | Trade: {row.get('trade_id', 'N/A')} | End-to-End Latency: {row['latency_ms']:.2f} ms")
        if len(clean_trades) > 5:
            print(f"... and {len(clean_trades) - 5} more clean trades processed simultaneously.")

# 5. Connect to Kafka and Start Streaming
print("Connecting to Kafka Stream...")
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "live_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON from the Kafka value column
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")

# Start the stream and apply our ML function to every new batch
query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()