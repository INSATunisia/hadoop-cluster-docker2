import happybase
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("TrainingALS").getOrCreate()
connection = happybase.Connection('172.22.0.5',9090)
ratings = connection.table('ratings')

rows = []
for key, data in ratings.scan():

    userId = int(key.decode('utf-8'))
    for movieId, rating in data.items():
        movieId = int(movieId.decode('utf-8').split(':')[1])
        rating = float(rating.decode('utf-8'))
        row = Row(userId=userId, movieId=movieId, rating=rating)
        #print(row)
        rows.append(row)

ratings=spark.createDataFrame(rows)
#print(ratings) 
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

user_recommendations = connection.table('user_recommendations')


# Convert the DataFrame to an rdd
rdd = userRecs.rdd.map(lambda row: (row["userId"], {"recommendations": row["recommendations"]}))

# Save the data to HBase
for row in rdd.collect():
    user_id = str(row[0])
    recommendations = row[1]["recommendations"]
    for rec in recommendations:
        movie_id = str(rec["movieId"])
        rating = float(rec["rating"])
        user_recommendations.put(b'user_id', {b'rating:movie_id': b'rating'})