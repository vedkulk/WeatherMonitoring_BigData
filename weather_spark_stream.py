from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, stddev, count,
    expr, sum, hour, date_format, to_timestamp, current_timestamp
)
from pyspark.sql.types import StructType, FloatType, StringType, TimestampType

# Create Spark Session with additional configurations
spark = SparkSession.builder \
    .appName("WeatherStreamingAnalytics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Define Schema matching the enhanced producer data
schema = StructType() \
    .add("temperature", FloatType()) \
    .add("humidity", FloatType()) \
    .add("wind_speed", FloatType()) \
    .add("pressure", FloatType()) \
    .add("feels_like", FloatType()) \
    .add("clouds", FloatType()) \
    .add("description", StringType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType())

# Read from Kafka
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data with timestamp handling
parsed_df = weather_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# Calculate real-time statistics (1-minute windows)
real_time_stats = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window("timestamp", "1 minute"),
        "city"
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("pressure").alias("avg_pressure"),
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp"),
        stddev("temperature").alias("temp_stddev")
    )

# Calculate hourly aggregations
hourly_stats = parsed_df \
    .withWatermark("timestamp", "2 hours") \
    .groupBy(
        window("timestamp", "1 hour"),
        "city"
    ) \
    .agg(
        avg("temperature").alias("hourly_avg_temp"),
        max("temperature").alias("hourly_max_temp"),
        min("temperature").alias("hourly_min_temp"),
        avg("humidity").alias("hourly_avg_humidity"),
        avg("wind_speed").alias("hourly_avg_wind"),
        count("*").alias("readings_count")
    )

# Calculate temperature anomalies
# Detect sudden temperature changes
temp_changes = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),
        "city"
    ) \
    .agg(
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp")
    ) \
    .withColumn("temp_change", expr("max_temp - min_temp")) \
    .filter("temp_change > 5")  # Alert on 5°C change within 5 minutes

# Write real-time statistics to console
query1 = real_time_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start()

# Write hourly statistics to parquet files
query2 = hourly_stats.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "weather_stats/hourly") \
    .option("checkpointLocation", "checkpoints/hourly") \
    .trigger(processingTime="1 hour") \
    .start()

# Write temperature anomalies back to Kafka
query3 = temp_changes.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "weather-anomalies") \
    .option("checkpointLocation", "checkpoints/anomalies") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for all queries to terminate
spark.streams.awaitAnyTermination()