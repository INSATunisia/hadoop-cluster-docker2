from kafka import KafkaProducer
import json
import time

# Define the Kafka topic to send data to
topic = "ratings-topic"

# Set up the Kafka producer
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send one sample movie rating to the Kafka topic


rating={
  "userId": 123,
  "movieId": 456,
  "rating": 4.5,
  "timestamp": 1619765467
}

producer.send(topic, value=rating)
time.sleep(1)
