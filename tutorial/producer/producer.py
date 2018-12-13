from kafka import KafkaProducer
import random
import json


producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii'),
)

print('Ctrl+c to stop')
while True:
    producer.send(
        'kafka-python-topic',
        random.randint(1, 999),
    )
