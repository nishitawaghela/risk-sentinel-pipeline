from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType , DoubleType
import pyspark

# fix- dynamic version not working - hence fixed the version
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

print(f"Booting Spark {pyspark.__version__}. Fetching Kafka package: {kafka_package}...")
#initialize spark session-automatically downloads java dependencies required for kafka
spark = SparkSession.builder \
    .appName("UBS_Sentinel_Silver_Layer") \
    .config("spark.jars.packages", kafka_package) \
    .getOrCreate()

#supress heavy spark logs so that we can see our print statements/output clearly
spark.sparkContext.setLogLevel("WARN")

print("Spark session initialized successfully! Listening to Kafka topic 'trades'...")

# Define the schema (must match the structure of the JSON our fastapi produces)
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("stock_ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("trader_id", StringType(), True),
    StructField("database_id", IntegerType(), True)
])

#read from kafka stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "live_trades") \
    .option("startingOffsets", "earliest") \
    .load()

#parse the JSON and apply the schema.kafka messages are in binary, so we need to cast them to string first
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), trade_schema).alias("data")
).select("data.*")

#output to console (for testing our connection)
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()



# 5. Output to Local Parquet Files (The Big Data Way)
# We use 'append' mode to just add new trades as they arrive.
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./data/silver_trades") \
    .option("checkpointLocation", "./data/checkpoints/silver_trades") \
    .start()

# Keep the stream running indefinitely
query.awaitTermination()