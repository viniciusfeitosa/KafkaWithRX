from mock import patch, Mock
from KafkaWithRX.consumer import RXConsumer


class Runner:
    v1 = None
    v2 = None
    v3 = None

    class MyImplementation(RXConsumer):

        consumer_topic = "test"

        def on_next(self, msg):
            Runner.v1 = msg

        def on_completed(self):
            Runner.v2 = 'Done!!!'

        def on_error(self, error):
            Runner.v3 = 'Error {}'.format(error)

    def __init__(self, *args, **kwargs):
        self.MyImplementation()


@patch('KafkaWithRX.consumer.RXConsumer.get_consumer')
def test_consumer(mocked_consumer):
    mocked_consumer.return_value = ['value']
    r = Runner()
    assert r.v1 == 'value'
    assert r.v2 == 'Done!!!'
    assert r.v3 is None


def test_consumer_fail():
    with patch('KafkaWithRX.consumer.RXConsumer.get_consumer') as mock:
        mock = Mock(side_effect=Exception('test'))
        mock()
        r = Runner()
        assert r.v3 == 'Error Test'
