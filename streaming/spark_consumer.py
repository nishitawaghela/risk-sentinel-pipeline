import os
import time
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import shap

# 1. Load our trained Random Forest Model
MODEL_PATH = '/Users/nishitawaghela/sentinel/ml_engine/saved_models/anomaly_detector_v3.pkl'
print("Loading ML Model...")
model = joblib.load(MODEL_PATH)

EXPECTED_FEATURES = ['price', 'volume', 'action_SELL', 'order_type_MARKET']

# 2. Initialize Spark Session with Kafka Support
spark = SparkSession.builder \
    .appName("SentinelRiskEngine") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Define Schema
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("stock_symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("ingestion_timestamp", DoubleType(), True)
])

# 4. Micro-Batch Processing Function
def process_batch(df, epoch_id):
    pdf = df.toPandas()
    
    if pdf.empty:
        return
    
    current_time = time.time()
    print(f"\n--- Processing Batch {epoch_id} | {len(pdf)} trades ---")
    
    # Feature Engineering
    if 'action' in pdf.columns:
        pdf['action_SELL'] = (pdf['action'] == 'SELL').astype(int)
    else:
        pdf['action_SELL'] = 0
        
    if 'order_type' in pdf.columns:
        pdf['order_type_MARKET'] = (pdf['order_type'] == 'MARKET').astype(int)
    else:
        pdf['order_type_MARKET'] = 0
    
    X = pdf[EXPECTED_FEATURES]
    
    # Predictions
    predictions = model.predict(X)
    pdf['risk_prediction'] = predictions
    
    # Latency
    if 'ingestion_timestamp' in pdf.columns:
        pdf['latency_ms'] = (current_time - pdf['ingestion_timestamp']) * 1000
    else:
        pdf['latency_ms'] = 0.0
    
    # Separate anomalies and clean
    anomalies = pdf[pdf['risk_prediction'] != 0]
    clean_trades = pdf[pdf['risk_prediction'] == 0]

    # SHAP — compute once for full batch
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    x_index_list = list(X.index)

    # Print fraud alerts with SHAP reason
    if not anomalies.empty:
        for index, row in anomalies.iterrows():
            predicted_class = int(row['risk_prediction'])
            shap_pos = x_index_list.index(index)
            shap_row = shap_values[shap_pos, :, predicted_class]
            top_feature = max(zip(EXPECTED_FEATURES, shap_row), key=lambda x: abs(x[1]))
            print(f"🚨 FRAUD FLAGGED | Trade: {row.get('trade_id', 'N/A')} | Type: {predicted_class} | Reason: {top_feature[0]} (SHAP: {top_feature[1]:.2f}) | Latency: {row['latency_ms']:.2f} ms")

    # Print clean trades
    if not clean_trades.empty:
        for index, row in clean_trades.head(5).iterrows():
            print(f"✅ CLEAN | Trade: {row.get('trade_id', 'N/A')} | Latency: {row['latency_ms']:.2f} ms")
        if len(clean_trades) > 5:
            print(f"... and {len(clean_trades) - 5} more clean trades processed simultaneously.")

# 5. Connect to Kafka
print("Connecting to Kafka Stream...")
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "live_trades") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")

query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()