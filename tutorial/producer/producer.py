from kafka import KafkaProducer, TopicPartition
from kafka.partitioner import RoundRobinPartitioner
from rx import Observer, Observable
import json
import functools


class Producer(Observer):
    def __init__(self, message_sender, compensatory_request=None):
        self.message_sender = message_sender
        self.compensatory_request = compensatory_request
        partitioner = RoundRobinPartitioner(partitions=[
            TopicPartition(topic=self.message_sender, partition=0),
            TopicPartition(topic=self.message_sender, partition=1)
        ])
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda m: json.dumps(m).encode('ascii'),
            partitioner=partitioner,
        )

    def on_next(self, value):
        self.producer.send(
            self.message_sender,
            value,
        )

    def on_completed(self):
        self.producer.flush()
        print('Done!!!')

    def on_error(self, error):
        if self.compensatory_request is None:
            print('Error raised: {}'.format(error))
        else:
            print('Error non-compensatory_request: {}'.format(error))


def producer_rx(message_sender, compensatory_request=None):
    def decorator_rx(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            source = Observable.of(func(*args, **kwargs))
            source.subscribe(Producer(message_sender))
        return wrapper
    return decorator_rx


@producer_rx(message_sender='kafka-python-topic')
def my_func(name):
    return {'name': name}


if __name__ == '__main__':
    print('Ctrl+c to stop')
    for i in range(1000):
        my_func('Pacheco a{}'.format(i))
        my_func('Pacheco b{}'.format(i))
