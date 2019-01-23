import json

from abc import ABCMeta
from kafka import KafkaConsumer
from rx import Observer


class RXConsumer(Observer):
    __metaclass__ = ABCMeta

    consumer_topic = None
    consumer_group = None

    def __init__(self, iterable=None):
        self.__iterable = iterable

    @property
    def iterable(self):
        if self.__iterable:
            return self.__iterable
        return KafkaConsumer(
            self.consumer_topic,
            bootstrap_servers=['localhost:29092'],
            key_deserializer=lambda m: m.decode('ascii'),
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            group_id=self.consumer_group,
        )

    def process_consumer(self, msg):
        return NotImplemented

    def on_next(self, msg):
        pass

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('Error:', error)


class MyImplementation1(RXConsumer):

    consumer_topic = 'kafka-python-topic'
    consumer_group = 'foo'

    def process_consumer(self, msg):
        import time
        time.sleep(msg.value['name'])
        print('Received: {}'.format(msg.value['name']))
