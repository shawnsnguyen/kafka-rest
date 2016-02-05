import sys
from unittest import TestCase
try:
    from queue import PriorityQueue
except ImportError:
    from Queue import PriorityQueue

from mock import Mock, patch, call

from .fixtures import MockClient
from kafka_rest.message import Message
from kafka_rest.events import FlushReason

class TestProducer(TestCase):
    def setUp(self):
        self.client = MockClient.with_basic_setup()
        self.producer = self.client.producer
        self.callback_mock = Mock(wraps=self.client.io_loop.add_callback)
        self.client.io_loop.add_callback = self.callback_mock

        self.io_loop_patch = patch('kafka_rest.producer.IOLoop.current')
        self.io_loop_mock = self.io_loop_patch.start()
        self.io_loop_mock.return_value = self.client.io_loop

        self.test_schema = {
            'type': 'record',
            'name': 'test_driver',
            'fields': [
                {'name': 'val', 'type': 'int'}
            ]
        }
        self.test_value = {'val': 1}

        self.client.schema_cache['test_driver']['value-id'] = 1
        self.client.schema_cache['test_driver']['key-id'] = 2

        self.producer.inflight_requests = {1: None}

    def tearDown(self):
        if not self.client.in_shutdown:
            self.client.shutdown(block=False)
        self.io_loop_patch.stop()

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
        fake_request._id = 1

        self.producer._send_batch_produce_request('test_driver', batch)
        call_args = fake_request.mock_calls[0][1]

        self.assertEqual(call_args[2], 10)
        self.assertEqual(call_args[3], 60)
        self.assertEqual(call_args[5], 'test_driver')
        self.assertEqual(call_args[6], batch)

    @patch('kafka_rest.producer.request_for_batch')
    def test_send_batch_produce_request_in_shutdown(self, fake_request):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        batch = [m1, m2]
        fake_request._id = 1

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
        self.client.schema_cache['test_driver']['value-id'] = None
        body = {'offsets': [], 'value_schema_id': 5, 'key_schema_id': 2}

        self.producer._handle_produce_success('test_driver', None, body)

        self.assertEqual(self.client.schema_cache['test_driver']['value-id'], 5)
        self.assertEqual(self.client.schema_cache['test_driver']['key-id'], 2)

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
        response.request._id = 1
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
        response.code = 599
        response.error = object()
        response.request = Mock()
        response.request._batch = [m1, m2]
        response.request._id = 1

        self.producer._handle_produce_response('test_driver', response)

        self.client.mock_for('response_5xx').assert_called_once_with('test_driver', response)
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
        response.request._id = 1
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
        response.request._id = 1
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
        response.request._id = 1
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

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    def test_flush_topic_resets_flush_timer_normally(self, fake_flush_timer):
        self.producer._flush_topic('test_driver', 'testing')
        fake_flush_timer.assert_called_once_with('test_driver')

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    def test_flush_topic_resets_flush_timer_under_circuit_breaker(self, fake_flush_timer):
        self.client.response_5xx_circuit_breaker.failure_count = sys.maxsize
        self.producer._flush_topic('test_driver', 'testing')
        fake_flush_timer.assert_called_once_with('test_driver')

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    def test_flush_topic_does_not_reset_flush_timer_in_shutdown(self, fake_flush_timer):
        self.client.shutdown(block=False)
        self.producer._flush_topic('test_driver', 'testing')
        self.assertFalse(fake_flush_timer.called)

    def test_flush_topic_sends_batches(self):
        self.client.flush_max_batch_size = 1
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        queue = self.client.message_queues['test_driver']
        for m in [m1, m2]:
            queue.put(m)

        self.producer._flush_topic('test_driver', 'testing')

        self.client.mock_for('flush_topic').assert_called_with('test_driver', 'testing')
        self.callback_mock.assert_has_calls([call(self.producer._send_batch_produce_request, 'test_driver', [m1]),
                                             call(self.producer._send_batch_produce_request, 'test_driver', [m2])])

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    def test_evaluate_queue_on_empty(self, fake_flush_timer):
        self.producer.evaluate_queue('test_driver', self.client.message_queues['test_driver'])
        fake_flush_timer.assert_called_with('test_driver')
        fake_flush_timer.reset_mock()

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    def test_evaluate_queue_on_empty_timer_set(self, fake_flush_timer):
        self.producer.flush_timers['test_driver'] = None
        self.producer.evaluate_queue('test_driver', self.client.message_queues['test_driver'])
        self.assertFalse(fake_flush_timer.called)

    @patch('kafka_rest.producer.AsyncProducer._reset_flush_timer')
    @patch('kafka_rest.producer.AsyncProducer._flush_topic')
    def test_evaluate_queue_on_length(self, fake_flush, fake_flush_timer):
        self.client.flush_length_threshold = 1
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        m2 = Message('test_driver', {'val': 2}, None, None, 0, 1)
        queue = self.client.message_queues['test_driver']
        for m in [m1, m2]:
            queue.put(m)

        self.producer.evaluate_queue('test_driver', self.client.message_queues['test_driver'])

        fake_flush.assert_called_with('test_driver', FlushReason.LENGTH)
        self.assertFalse(fake_flush_timer.called)
