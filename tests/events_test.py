from unittest import TestCase
from mock import Mock, ANY

from kafka_rest.events import EventRegistrar

def _broken_handler(*args, **kwargs):
    raise ValueError('wonk wonk')

class TestEventRegistrar(TestCase):
    def setUp(self):
        self.registrar = EventRegistrar()

    def test_emit_with_no_handlers(self):
        self.registrar.emit('test')

    def test_handlers_called_in_order(self):
        tracker = []
        handler_1 = lambda: tracker.append(1)
        handler_2 = lambda: tracker.append(2)
        self.registrar.register('test', handler_1)
        self.registrar.register('test', handler_2)

        self.registrar.emit('test')

        self.assertEqual(tracker, [1, 2])

    def test_handler_exception_emits_exception_event(self):
        self.registrar.register('test', _broken_handler)
        self.exception_mock = Mock()
        self.registrar.register('event_handler_exception', self.exception_mock)

        self.registrar.emit('test')

        self.exception_mock.assert_called_once_with('test', ANY)

    def test_broken_exception_event_does_not_infinite_loop(self):
        self.registrar.register('test', _broken_handler)
        self.registrar.register('event_handler_exception', _broken_handler)

        self.registrar.emit('test')
