from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logger = logging.getLogger("org")
logger.setLevel(logging.FATAL)

#.config("spark.local.‌​dir","D:/spark_temp") \
spark = SparkSession.builder.appName("KafkaStructuredStreaming").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.local.‌​dir", "D:/spark_temp").getOrCreate()

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'weatherkafkatopic'

json_schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("wind_mph", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("precip_mm", DoubleType(), True),
    StructField("condition", StringType(), True),
])


# Read data from Kafka topic as a streaming DataFrame
raw_data_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convert the value column from Kafka into a string
raw_data_stream = raw_data_stream.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and apply the schema
structured_data_stream = raw_data_stream.select(
    from_json(col("value"), json_schema).alias("data")
).select("data.*")

# Define the Delta Lake storage path
delta_path = "../Time-Series-Forecasting-Spark-Kafka-for-Weather-Data-main/delta_lake/delta_lake_table_4"  

# Write the streaming DataFrame to Delta Lake
query = structured_data_stream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "../Time-Series-Forecasting-Spark-Kafka-for-Weather-Data-main/delta_lake/checkpoint_4") \
    .start(delta_path)

# Await termination of the query
query.awaitTermination()

# spark.stop()