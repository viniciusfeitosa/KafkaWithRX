from kafka import KafkaConsumer
from rx import (
    Observer,
    Observable,
)
import json


class Consumer(Observer):

    def on_next(self, value):
        print('Received {}'.format(value.value))

    def on_completed(self):
        print('Done!!!')

    def on_error(self, error):
        print('Error raised: {}'.format(error))


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'kafka-python-topic',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
    )

    source = Observable.from_(consumer)
    source.subscribe(Consumer())
