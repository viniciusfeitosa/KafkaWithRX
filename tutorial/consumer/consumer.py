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
        group_id = 'rx-{}'.format(self.__class__.__name__)
        print('group ID {}'.format(group_id))
        print('topic {}'.format(self.consumer_topic))
        self.consumer = KafkaConsumer(
            self.consumer_topic,
            bootstrap_servers=['localhost:29092'],
            key_deserializer=lambda m: m.decode('ascii'),
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            group_id=group_id,
        )
        source = Observable.from_(self.consumer)
        source.subscribe(self)


class MyImplementation(RXConsumer):

    consumer_topic = 'kafka-python-topic'

    def on_next(self, msg):
        print('Received key {} and value {}'.format(msg.key, msg.value))

    def on_completed(self):
        print('Done!!!')

    def on_error(self, error):
        print('Error raised: {}'.format(error))


if __name__ == '__main__':
    MyImplementation()
