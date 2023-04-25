from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Read a Parquet file into a dataframe
path = "output/part-00000-4751708f-87cc-4a83-bd7a-1b07efd30a51-c000.snappy.parquet"
df = spark.read.parquet(path)

# Do some operations on the dataframe
df.show()
