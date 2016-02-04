"""These tests attempt to test the control flow of the producer as it
processes events. These are more complex than unit tests but they are
necessary because the producer's methods contain individual processing
logic (i.e. the flush topic method flushes a topic) but also are responsible
for registering other callbacks to continue operating the producer correctly
(i.e. flush topic is also responsible for resetting the flush timer)."""

from unittest import TestCase
from mock import Mock
import json

from kafka_rest.message import Message
from .fixtures import MockClient, Callback

class TestFlow(TestCase):
    def setUp(self):
        self.client = MockClient.with_basic_setup()
        self.client.install_mock_io_loop()
        self.producer = self.client.producer
        self.io_loop = self.client.io_loop

        self.test_schema = {
            'type': 'record',
            'name': 'test_driver',
            'fields': [
                {'name': 'val', 'type': 'int'}
            ]
        }
        self.test_value = {'val': 1}

        self.client.schema_cache['test_driver']['value'] = self.test_schema
        self.client.schema_cache['test_driver']['value-id'] = 1

        self.producer.inflight_requests = {1: None}

    def tearDown(self):
        if not self.client.in_shutdown:
            self.client.shutdown(block=False)
        self.client.teardown_mock_io_loop()

    def make_response(self, response_attrs=None, request_attrs=None):
        response = Mock()
        request = Mock()
        response.code = 200
        response.error = None
        response.body = json.dumps({'offsets': [{}]})
        request._id = 1
        request._topic = 'test_driver'
        request._batch = [Message('test_driver', self.test_value, None, None, 0, 1)]
        for k, v in (response_attrs or {}).items():
            if k == 'body':
                v = json.dumps(v)
            setattr(response, k, v)
        for k, v in (request_attrs or {}).items():
            setattr(request, k, v)
        response.request = request
        return response

    def test_produce_through_evaluate_no_flush(self):
        self.client.produce('test_driver', self.test_value, self.test_schema)

        self.assertEqual(self.io_loop.next_callback,
                         Callback(self.producer.evaluate_queue,
                                  ('test_driver', self.client.message_queues['test_driver']),
                                  {}))
        self.io_loop.run_next_callback()

        self.assertEqual(self.io_loop.next_later.seconds, self.client.flush_time_threshold_seconds)
        self.assertEqual(self.io_loop.next_later.callback,
                         Callback(self.producer._flush_topic,
                                  ('test_driver', 'time'),
                                  {}))

        self.io_loop.pop_later()
        self.assertTrue(self.io_loop.finished)

    def test_produce_through_evaluate_into_flush(self):
        self.client.flush_length_threshold = 0
        self.client.produce('test_driver', self.test_value, self.test_schema)

        self.assertEqual(self.io_loop.next_callback,
                         Callback(self.producer.evaluate_queue,
                                  ('test_driver', self.client.message_queues['test_driver']),
                                  {}))
        self.io_loop.run_next_callback()

        self.assertEqual(self.io_loop.next_callback.fn, self.producer._send_batch_produce_request)
        expected_message = Message('test_driver', self.test_value, None, None, 0, 1)
        self.assertTrue(self.io_loop.next_callback.args[1][0].true_equals(expected_message))


        self.assertEqual(self.io_loop.next_later.seconds, self.client.flush_time_threshold_seconds)
        self.assertEqual(self.io_loop.next_later.callback,
                         Callback(self.producer._flush_topic,
                                  ('test_driver', 'time'),
                                  {}))

        self.io_loop.pop_callback()
        self.io_loop.pop_later()

        self.assertTrue(self.io_loop.finished)

    def test_flush_through_send(self):
        m1 = Message('test_driver', {'val': 1}, None, None, 0, 1)
        queue = self.client.message_queues['test_driver']
        queue.put(m1)

        self.producer._flush_topic('test_driver', 'test')

        self.assertEqual(self.io_loop.next_callback.fn, self.producer._send_batch_produce_request)
        self.assertTrue(self.io_loop.next_callback.args[1][0].true_equals(m1))

        self.io_loop.run_next_callback()

        # Response handler callback from Tornado's HTTP client, this function
        # is defined inline so we can't test against it very well
        self.assertEqual(self.io_loop.next_callback.fn.__name__, 'handle_response')
        self.io_loop.pop_callback()

        self.assertEqual(self.io_loop.next_later.callback,
                         Callback(self.producer._flush_topic,
                                  ('test_driver', 'time'),
                                  {}))
        self.io_loop.pop_later()

        self.assertTrue(self.io_loop.finished)

    def test_response_through_success_no_failures(self):
        response = self.make_response()

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(0, self.client.retry_queues['test_driver'].qsize())

    def test_response_through_success_with_some_nonretriable_failures(self):
        response = self.make_response(
            response_attrs={
                'body': {'offsets': [{'error_code': 1, 'message': 'Nonretriable'}]}})

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(0, self.client.retry_queues['test_driver'].qsize())

    def test_response_through_success_with_some_retriable_failures(self):
        response = self.make_response(
            response_attrs={
                'body': {'offsets': [{'error_code': 2, 'message': 'Retriable'}]}})

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(1, self.client.retry_queues['test_driver'].qsize())

    def test_response_through_success_with_transport_error(self):
        response = self.make_response(
            response_attrs={'error': 'anything truthy'})

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(1, self.client.retry_queues['test_driver'].qsize())

    def test_response_through_success_with_retriable_full_error(self):
        response = self.make_response(
            response_attrs={'code': 500, 'body': {'error_code': 50001, 'message': 'Retriable'}})

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(1, self.client.retry_queues['test_driver'].qsize())

    def test_response_through_success_with_nonretriable_full_error(self):
        response = self.make_response(
            response_attrs={'code': 500, 'body': {'error_code': 50101, 'message': 'Nonretriable'}})

        self.producer._handle_produce_response('test_driver', response)

        self.assertTrue(self.io_loop.finished)
        self.assertEqual(0, self.client.retry_queues['test_driver'].qsize())
