from kafka import KafkaConsumer
import json
import csv

# Define the Kafka topic to consume data from
# topic = "ratings-topic"
topic = "ratings-topic"

# Set up the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=["localhost:9092"],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Define the path of the CSV file to store the ratings
csv_file_path = "new_ratings.csv"

# Open the CSV file in append mode
with open(csv_file_path, mode='a', newline='') as ratings_file:
    # Create a CSV writer object
    writer = csv.writer(ratings_file)

    # Consume ratings from the Kafka topic
    for message in consumer:
        rating = message.value
        print(f"Received rating: {rating}")

        # Write the rating to the CSV file
        writer.writerow([rating['userId'], rating['movieId'], rating['rating'], rating['timestamp']])

