import sys
from unittest import TestCase
try:
    from queue import PriorityQueue
except ImportError:
    from Queue import PriorityQueue

from mock import Mock, patch, call

from .fixtures import MockClient
from kafka_rest.message import Message

class TestProducer(TestCase):
    def setUp(self):
        self.client = MockClient.with_basic_setup()
        self.producer = self.client.producer
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

        self.client.schema_cache['value']['test_driver'] = 1
        self.client.schema_cache['key']['test_driver'] = 2

    def tearDown(self):
        if not self.client.in_shutdown:
            self.client.shutdown(block=True)

    def test_message_batches_from_queue_single_batch(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        queue = self.client.message_queues['test_driver']
        for m in [m1, m2]:
            queue.put(m)

        result = list(self.producer._message_batches_from_queue(queue))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], [m1, m2])

    def test_message_batches_from_queue_multiple_batches(self):
        self.client.flush_max_batch_size = 1
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        queue = self.client.message_queues['test_driver']
        for m in [m1, m2]:
            queue.put(m)

        result = list(self.producer._message_batches_from_queue(queue))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [m1])
        self.assertEqual(result[1], [m2])

    def test_message_batches_from_retry_queue(self):
        m1 = Message('test_driver', {'val': 1}, None, None, sys.maxsize, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        queue = self.client.retry_queues['test_driver']
        for m in [m1, m2]:
            queue.put(m)

        result = list(self.producer._message_batches_from_queue(queue))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], [m2])

    @patch('kafka_rest.producer.request_for_batch')
    def test_send_batch_produce_request(self, fake_request):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        batch = [m1, m2]

        self.producer._send_batch_produce_request('test_driver', batch)
        call_args = fake_request.mock_calls[0][1]

        self.assertEqual(call_args[2], 60)
        self.assertEqual(call_args[3], 60)
        self.assertEqual(call_args[5], 'test_driver')
        self.assertEqual(call_args[6], batch)

    @patch('kafka_rest.producer.request_for_batch')
    def test_send_batch_produce_request_in_shutdown(self, fake_request):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        batch = [m1, m2]

        self.client.in_shutdown = True
        self.producer._send_batch_produce_request('test_driver', batch)
        call_args = fake_request.mock_calls[0][1]

        self.assertEqual(call_args[2], 0)
        self.assertEqual(call_args[3], 0)
        self.assertEqual(call_args[5], 'test_driver')
        for idx, m in enumerate(batch):
            self.assertTrue(m.true_equals(call_args[6][idx]))

    def test_queue_message_for_retry(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        self.producer._queue_message_for_retry('test_driver', m1)
        retry_message = self.client.retry_queues['test_driver'].get()

        self.assertEqual(retry_message.attempt_number,
                         m1.attempt_number + 1)
        self.assertNotEqual(retry_message.retry_after_time,
                            m1.retry_after_time)
        self.client.mock_for('retry_message').assert_called_once_with('test_driver',
                                                                      retry_message)

    def test_queue_message_for_retry_when_full(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        self.client.retry_queues['test_driver'] = PriorityQueue(maxsize=1)
        self.client.retry_queues['test_driver'].put('whatever')

        self.producer._queue_message_for_retry('test_driver', m1)
        self.client.mock_for('drop_message').assert_called_once_with('test_driver',
                                                                     m1,
                                                                     'retry_queue_full')

    def test_queue_message_for_retry_nonretriable(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 10)
        self.producer._queue_message_for_retry('test_driver', m1)
        self.client.mock_for('drop_message').assert_called_once_with('test_driver',
                                                                     m1,
                                                                     'max_retries_exceeded')

    def test_handle_produce_success_stores_value_schema_id(self):
        self.client.schema_cache['value']['test_driver'] = None
        body = {'offsets': [], 'value_schema_id': 5, 'key_schema_id': 2}

        self.producer._handle_produce_success('test_driver', None, body)

        self.assertEqual(self.client.schema_cache['value']['test_driver'], 5)
        self.assertEqual(self.client.schema_cache['key']['test_driver'], 2)

    def test_handle_produce_success_simple_success(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        response = Mock()
        response.request = Mock()
        response.request._batch = [m1]
        body = {'offsets': [{}], 'value_schema_id': 1, 'key_schema_id': 2}

        self.producer._handle_produce_success('test_driver', response, body)

        self.client.mock_for('produce_success').assert_called_once_with('test_driver',
                                                                        [(m1, {})],
                                                                        [])

    @patch('kafka_rest.producer.AsyncProducer._queue_message_for_retry')
    def test_handle_produce_success_mixed_success(self, fake_retry):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        m3 = Message('test_driver', {'val': 3}, None, None, 0, 1)
        offsets = [{},
                   {'error_code': 1, 'message': 'Nonretriable'},
                   {'error_code': 2, 'message': 'Retriable'}]
        response = Mock()
        response.request = Mock()
        response.request._batch = [m1, m2, m3]
        body = {'offsets': offsets,
                'value_schema_id': 1,
                'key_schema_id': 2}

        self.producer._handle_produce_success('test_driver', response, body)

        self.client.mock_for('produce_success').assert_called_once_with('test_driver',
                                                                        [(m1, {})],
                                                                        [(m2, offsets[1]),
                                                                         (m3, offsets[2])])
        self.client.mock_for('drop_message').assert_called_once_with('test_driver',
                                                                     m2,
                                                                     'nonretriable')
        fake_retry.assert_called_once_with('test_driver', m3)

    @patch('kafka_rest.producer.AsyncProducer._queue_message_for_retry')
    def test_handle_produce_response_transport_error(self, fake_retry):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        response = Mock()
        response.error = object()
        response.request = Mock()
        response.request._batch = [m1, m2]

        self.producer._handle_produce_response('test_driver', response)

        self.client.mock_for('transport_error').assert_called_once_with('test_driver',
                                                                        response.error)
        fake_retry.assert_has_calls([call('test_driver', m1),
                                     call('test_driver', m2)])

    @patch('kafka_rest.producer.AsyncProducer._handle_produce_success')
    @patch('kafka_rest.producer.json_decode')
    def test_handle_produce_response_on_200(self, fake_decode, fake_handle_success):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        response = Mock()
        response.code = 200
        response.error = False
        response.request = Mock()
        response.request._batch = [m1, m2]
        body = {'offsets': [{}, {}],
                'value_schema_id': 1,
                'key_schema_id': 2}
        fake_decode.return_value = body

        self.producer._handle_produce_response('test_driver', response)

        fake_handle_success.assert_called_once_with('test_driver',
                                                    response,
                                                    body)

    @patch('kafka_rest.producer.AsyncProducer._queue_message_for_retry')
    @patch('kafka_rest.producer.json_decode')
    def test_handle_produce_response_on_retriable(self, fake_decode, fake_retry):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        response = Mock()
        response.code = 500
        response.error = False
        response.request = Mock()
        response.request._batch = [m1, m2]
        body = {'offsets': [{}, {}],
                'value_schema_id': 1,
                'key_schema_id': 2,
                'error_code': 50001,
                'message': 'Retriable'}
        fake_decode.return_value = body

        self.producer._handle_produce_response('test_driver', response)

        fake_retry.assert_has_calls([call('test_driver', m1),
                                     call('test_driver', m2)])


    @patch('kafka_rest.producer.AsyncProducer._queue_message_for_retry')
    @patch('kafka_rest.producer.json_decode')
    def test_handle_produce_response_on_nonretriable(self, fake_decode, fake_retry):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        response = Mock()
        response.code = 500
        response.error = False
        response.request = Mock()
        response.request._batch = [m1, m2]
        body = {'offsets': [{}, {}],
                'value_schema_id': 1,
                'key_schema_id': 2,
                'error_code': 50101,
                'message': 'Nonretriable'}
        fake_decode.return_value = body

        self.producer._handle_produce_response('test_driver', response)

        calls = [call('test_driver', m1, 'nonretriable'),
                 call('test_driver', m2, 'nonretriable')]
        self.client.mock_for('drop_message').assert_has_calls(calls)
