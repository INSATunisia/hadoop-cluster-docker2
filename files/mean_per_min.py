from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("KafkaStream").getOrCreate()

# Define the schema for the rating data
schema = StructType([
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", LongType())
])

# Read streaming data from Kafka topic
ratings = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ratings-topic") \
    .option("startingOffsets", "earliest") \
    .option('includeTimestamp', 'true')\
    .load()

# Parse the value column of Kafka message as JSON and apply the schema
parsed_ratings = ratings \
    .select(from_json(col("value").cast("string"), schema).alias("data"),ratings.timestamp) \
    .selectExpr("data.userId", "data.movieId", "data.rating", "timestamp")


# Aggregate the rating by averaging over every 1 minute window
#avg_ratings = parsed_ratings \
#    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
#    .withWatermark("timestamp", "1 minute") \
#    .groupBy(window(col("timestamp"), "1 minute")) \
#    .agg(avg("rating").alias("mean_rating"))
avg_ratings = parsed_ratings \
        .groupBy(window("timestamp", "1 minute", "1 minute")).agg(avg("rating").alias("mean_rating"))
# Write the streaming output to console
query = avg_ratings \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()