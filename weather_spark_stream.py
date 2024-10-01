from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, FloatType, StringType

# Create Spark Session
spark = SparkSession.builder \
    .appName("WeatherStreamingApp") \
    .getOrCreate()

# Define Schema
schema = StructType() \
    .add("temperature", FloatType()) \
    .add("humidity", FloatType()) \
    .add("wind_speed", FloatType()) \
    .add("city", StringType())

# Read from Kafka
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Convert Kafka message value to JSON format
weather_json_df = weather_df.selectExpr("CAST(value AS STRING)")

# Parse JSON data
weather_parsed_df = weather_json_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Write processed data back to Kafka (optional)
weather_parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
