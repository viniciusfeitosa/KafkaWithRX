from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.structs import OffsetAndMetadata, TopicPartition
from rx import (
    Observer,
    Observable,
)
import json


class Consumer(Observer):

    def __init__(self, consumer):
        self.consumer = consumer

    def on_next(self, msg):
        tp = TopicPartition(msg.topic, msg.partition)
        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
        print('Received {}'.format(msg.value))
        self.consumer.commit(offsets=offsets)

    def on_completed(self):
        print('Done!!!')

    def on_error(self, error):
        print('Error raised: {}'.format(error))


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'kafka-python-topic',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        partition_assignment_strategy=[RoundRobinPartitionAssignor],
        group_id='my_group'
    )

    source = Observable.from_(consumer)
    source.subscribe(Consumer(consumer))
