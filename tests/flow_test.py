"""These tests attempt to test the control flow of the producer as it
processes events. These are more complex than unit tests but they are
necessary because the producer's methods contain individual processing
logic (i.e. the flush topic method flushes a topic) but also are responsible
for registering other callbacks to continue operating the producer correctly
(i.e. flush topic is also responsible for resetting the flush timer)."""

from unittest import TestCase

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

    def tearDown(self):
        if not self.client.in_shutdown:
            self.client.shutdown(block=False)
        self.client.teardown_mock_io_loop()

    def test_produce_through_evaluate_no_flush(self):
        self.client.produce('test_driver', self.test_value, self.test_schema)
        self.assertEqual(self.io_loop.next_callback,
                         Callback(self.producer.evaluate_queue,
                                  ('test_driver', self.client.message_queues['test_driver']),
                                  {}))

        self.io_loop.run_next()

        self.assertEqual(self.io_loop.next_later.seconds, self.client.flush_time_threshold_seconds)
        self.assertEqual(self.io_loop.next_later.callback,
                         Callback(self.producer._flush_topic,
                                  ('test_driver', 'time'),
                                  {}))
