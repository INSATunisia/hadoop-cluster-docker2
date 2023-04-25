from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import IntegerType, FloatType, StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.ml.recommendation import ALSModel

# Define the schema for the incoming data
rating_schema = StructType([
    StructField("userId", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", FloatType()),
    StructField("timestamp", LongType())
])


# Define the Spark session
spark = SparkSession.builder.appName("UpdateALS").getOrCreate()

# Load the pre-trained ALS model
model = ALSModel.load("trained_als_model")

# Read the data from the Kafka topic as a streaming DataFrame
df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ratings-topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(col("key").cast("string"), col("value").cast("string")) \
        .select(from_json(col("value"), rating_schema).alias("data")) \
        .selectExpr("data.userId", "data.movieId", "data.rating", "data.timestamp")


# Generate recs with the incoming ratings
user_subset_recs = model.recommendForUserSubset(df.select(model.getUserCol()).distinct(), 10)


# Save recs
#print('userrecs', user_subset_recs.show())


# save, partition by userId column
user_subset_recs.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/ccccheckpoint") \
        .option("path", "output/new") \
        .start() \
        .awaitTermination()

