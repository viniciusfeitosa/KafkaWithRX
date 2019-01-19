import json

from kafka import KafkaConsumer


def get_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:29092'],
        key_deserializer=lambda m: m.decode('ascii'),
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        group_id='rx-{}'.format(group_id),
    )
