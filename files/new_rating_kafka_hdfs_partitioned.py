# This code reads new incoming ratings from kafka topic, and then saves them in hdfs, partitioned by userId

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


# Define the schema for the incoming data
rating_schema = StructType([
    StructField("userId", StringType()),
    StructField("movieId", StringType()),
    StructField("rating", DoubleType()),
    StructField("timestamp", LongType())
])


# Define the Spark session
spark = SparkSession.builder.appName("RatingsApp").getOrCreate()


# Read the data from the Kafka topic as a streaming DataFrame
df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ratings-topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(col("key").cast("string"), col("value").cast("string")) \
        .select(from_json(col("value"), rating_schema).alias("data")) \
        .selectExpr("data.userId", "data.movieId", "data.rating", "data.timestamp")

# save, partition by userId column
df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/xheckpoint") \
        .option("path", "ratings") \
        .partitionBy("userId") \
        .start() \
        .awaitTermination()
