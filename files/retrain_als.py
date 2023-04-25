from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import IntegerType, FloatType, LongType
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("TrainALS").getOrCreate()


# Get ratings from csv
lines = spark.read.option("header", "true").csv("ratings/ratings.csv").rdd
# Convert data from rdd into dataframe
ratingsRDD = lines.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2]), timestamp=int(p[3])))
ratings=spark.createDataFrame(ratingsRDD)

# Get ratings from parquets
ratings_p = spark.read.parquet("ratings")
ratings_p = ratings_p.select(col("userId").cast(IntegerType()), 
                             col("movieId").cast(IntegerType()), 
                             col("rating").cast(FloatType()), 
                             col("timestamp").cast(LongType()))

# Concatenate the two dataframes
ratings = ratings.union(ratings_p)




#print("ratings1", ratings.count())
#print("ratings2", ratings_df.count())
#print("ratings3", ratings.count())

# Data Cleaning
# Add a row number column based on timestamp within each user-movie partition
window_spec = Window.partitionBy("userId", "movieId").orderBy("timestamp")
ratings = ratings.withColumn("row_num", row_number().over(window_spec))

# Keep only the rows with the most recent timestamp within each user-movie partition
ratings = ratings.filter(ratings.row_num == 1).drop("row_num")

#   Train the ALS model and generate recommendations

# Train/Test split
(training, test) = ratings.randomSplit([0.8, 0.2])

# Train ALS model
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

# Generate predictions
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Generate top 10 recommendations for every user, in a distributed manner
userRecs = model.recommendForAllUsers(10)

# Save recs
userRecs.write.save("output")

# Save trained model
model.save("trained_als_model")
