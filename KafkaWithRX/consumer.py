from abc import ABCMeta
from kafka import KafkaConsumer
from rx import (
    Observer,
    Observable,
)
import json


class RXConsumer(Observer):
    __metaclass__ = ABCMeta

    consumer_topic = None

    def __init__(self):
        self.consumer = self.get_consumer()
        source = Observable.from_(self.consumer)
        source.subscribe(self)

    def get_consumer(self):
        return KafkaConsumer(
            self.consumer_topic,
            bootstrap_servers=['localhost:29092'],
            key_deserializer=lambda m: m.decode('ascii'),
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            group_id='rx-{}'.format(self.__class__.__name__),
        )

    def process_consumer(self, msg):
        return NotImplemented

    def on_next(self, msg):
        try:
            self.process_consumer(msg)
        except Exception as e:
            self.on_error(e)

    def on_completed(self):
        self.consumer.close()

    def on_error(self, error):
        self.consumer.close()
        print('Error:', error)


class MyImplementation(RXConsumer):

    consumer_topic = 'kafka-python-topic'

    def process_consumer(self, msg):
        raise KeyError('bla')
        print('Received key {} and value {}'.format(msg.key, msg.value))


if __name__ == '__main__':
    MyImplementation()
