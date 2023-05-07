from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import IntegerType, FloatType, LongType
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import starbase

spark = SparkSession.builder.appName("TrainALS").getOrCreate()


# Get ratings from csv
lines = spark.read.option("header", "true").csv("./ml-latest-small/ratings.csv").rdd
# Convert data from rdd into dataframe
ratingsRDD = lines.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2]), timestamp=int(p[3])))
ratings=spark.createDataFrame(ratingsRDD)



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
print(userRecs)

# Create a connection to HBase
conn = starbase.Connection("172.22.0.5", "8080")

# Define the HBase table name
table_name = "user_recommendations"

user_recommendations = conn.table('user_recommendations')

if (user_recommendations.exists()):
    print('Dropping existing user_recommendations table\n')
    user_recommendations.drop()

user_recommendations.create('movie')

# Convert the DataFrame to an rdd
rdd = userRecs.rdd.map(lambda row: (row["userId"], {"recommendations": row["recommendations"]}))

print(rdd)

# Save the data to HBase
for row in rdd.collect():
    user_id = str(row[0])
    recommendations = row[1]["recommendations"]
    for rec in recommendations:
        movie_id = str(rec["movieId"])
        rating = str(rec["rating"])
        user_recommendations.insert(user_id, {"movie": {movie_id: rating}})
