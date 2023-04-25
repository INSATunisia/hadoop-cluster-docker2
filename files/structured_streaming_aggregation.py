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
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "ratings-topic") \
  .load()


# Split the lines into words
words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
