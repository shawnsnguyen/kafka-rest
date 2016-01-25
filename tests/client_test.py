from Queue import Queue
from unittest import TestCase
import time

from mock import Mock

from .fixtures import MockClient
from kafka_rest.exceptions import KafkaRESTShutdownException
from kafka_rest.message import Message

class TestClient(TestCase):
    def setUp(self):
        self.client = MockClient.with_basic_setup()
        self.callback_mock = Mock(wraps=self.client.io_loop.add_callback)
        self.client.io_loop.add_callback = self.callback_mock

        self.test_schema = {
            'type': 'record',
            'name': 'test_driver',
            'fields': [
                {'name': 'val', 'type': 'int'}
            ]
        }
        self.test_value = {'val': 1}

    def tearDown(self):
        if not self.client.in_shutdown:
            self.client.shutdown(block=True)

    def test_initial_produce(self):
        self.client.produce('test_driver', self.test_value, self.test_schema)
        expected_message = Message('test_driver', self.test_value, None, None, 0, 1)

        self.assertEqual(self.client.schema_cache['value']['test_driver'], self.test_schema)
        self.assertIsNone(self.client.schema_cache['key'].get('test_driver'))
        self.assertEqual(1, self.client.message_queues['test_driver'].qsize())
        self.client.mock_for('produce').assert_called_once_with(expected_message)
        self.callback_mock.assert_called_once_with(self.client.producer.evaluate_queue,
                                                   'test_driver',
                                                   self.client.message_queues['test_driver'])

    def test_produce_raises_in_shutdown(self):
        self.client.shutdown(block=True)
        with self.assertRaises(KafkaRESTShutdownException):
            self.client.produce('test_driver', self.test_value, self.test_schema)

    def test_produce_with_full_queue(self):
        self.client.message_queues['test_driver'] = Queue(maxsize=1)
        self.client.message_queues['test_driver'].put('whatever')

        self.client.produce('test_driver', self.test_value, self.test_schema)
        expected_message = Message('test_driver', self.test_value, None, None, 0, 1)

        self.assertEqual(1, self.client.message_queues['test_driver'].qsize())
        self.assertTrue(self.client.mock_for('drop_message').called_with('test_driver',
                                                                         expected_message,
                                                                         'primary_queue_full'))

    def test_shutdown(self):
        self.client.shutdown()
        self.callback_mock.assert_called_once_with(self.client.producer.start_shutdown)

    def test_shutdown_blocking(self):
        self.client.shutdown(block=True)
        self.callback_mock.assert_any_call(self.client.producer.start_shutdown)
        self.assertFalse(self.client.producer_thread.is_alive())
