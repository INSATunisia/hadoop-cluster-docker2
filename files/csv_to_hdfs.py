from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVToHDFS").getOrCreate()


# Get ratings from csv
lines = spark.read.option("header", "true").csv("ratings/ratings.csv").rdd
# Convert data from rdd into dataframe
ratingsRDD = lines.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2]), timestamp=int(p[3])))
ratings=spark.createDataFrame(ratingsRDD)

# Get ratgins from parquets
