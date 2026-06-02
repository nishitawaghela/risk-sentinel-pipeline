import os
import json
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
for message in consumer:
    # 1. Decode the message
    trade_data = json.loads(message.value().decode('utf-8'))
    ingestion_time = trade_data.get('ingestion_timestamp')
    
    # 2. Run your Random Forest Prediction
    features = extract_features(trade_data) # (However you format your data for the model)
    fraud_prediction = rf_model.predict(features)
    
    # 3. STOP THE CLOCK: Calculate End-to-End Latency in milliseconds
    completion_time = time.time()
    
    if ingestion_time:
        total_latency_ms = (completion_time - ingestion_time) * 1000
        
        if fraud_prediction[0] == 1:
            print(f"🚨 FRAUD FLAGGED | Trade: {trade_data['trade_id']} | End-to-End Latency: {total_latency_ms:.2f} ms")
        else:
            print(f"✅ CLEAN | Trade: {trade_data['trade_id']} | End-to-End Latency: {total_latency_ms:.2f} ms")
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
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("stock_symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("order_type", StringType(), True)
])

# 4. The Micro-Batch Processing Function (Where the magic happens)
def process_batch(df, epoch_id):
    # Convert the Spark DataFrame to a Pandas DataFrame for the ML model
    pdf = df.toPandas()
    
    if pdf.empty:
        return
    
    print(f"\n--- Processing Batch {epoch_id} | {len(pdf)} trades ---")
    
    # Feature Engineering: One-Hot Encode just like we did in training
    pdf['action_SELL'] = (pdf['action'] == 'SELL').astype(int)
    pdf['order_type_MARKET'] = (pdf['order_type'] == 'MARKET').astype(int)
    
    # Ensure all required columns are present
    X = pdf[EXPECTED_FEATURES]
    
    # Make Predictions
    predictions = model.predict(X)
    pdf['risk_prediction'] = predictions
    
    # Filter and alert on anomalies (Any prediction other than 0/'Normal')
    # Assuming 'Normal' is 0. If your label map is different, adjust this logic.
    anomalies = pdf[pdf['risk_prediction'] != 0] 
    
    if not anomalies.empty:
        print("🚨 FRAUD ALERT DETECTED 🚨")
        print(anomalies[['trade_id', 'stock_symbol', 'action', 'risk_prediction']])
    else:
        print("✅ All trades normal.")

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