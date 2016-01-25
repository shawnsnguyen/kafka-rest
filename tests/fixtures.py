import types
from collections import defaultdict

from mock import Mock

from kafka_rest.client import KafkaRESTClient

def _mock_emitter(obj, event, *args, **kwargs):
    obj.event_mocks[event](*args, **kwargs)

class MockClient(KafkaRESTClient):
    @classmethod
    def with_basic_setup(cls):
        instance = cls(None, None, shutdown_timeout_seconds=0)
        instance.event_mocks = defaultdict(lambda: Mock())
        instance.registrar.emit = types.MethodType(_mock_emitter, instance)
        return instance

    def reset_mocks(self):
        for mock in self.event_mocks.values():
            mock.reset_mock()

    def mock_for(self, event):
        return self.event_mocks[event]
