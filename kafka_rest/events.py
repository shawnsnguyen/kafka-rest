import logging
from collections import defaultdict

logger = logging.getLogger('kafka_rest.events')

class FlushReason(object):
    LENGTH = 'length'
    TIME = 'time'
    SHUTDOWN = 'shutdown'

class DropReason(object):
    NONRETRIABLE = 'nonretriable'
    PRIMARY_QUEUE_FULL = 'primary_queue_full'
    RETRY_QUEUE_FULL = 'retry_queue_full'
    MAX_RETRIES_EXCEEDED = 'max_retries_exceeded'

class EventRegistrar(object):
    """
    Handles registration of callable event handlers which are
    executed in sequence when events are emitted.
    """
    def __init__(self, debug=False):
        self.handlers = defaultdict(list)
        self.debug = debug

    def register(self, event, handler):
        self.handlers[event].append(handler)

    def emit(self, event, *args, **kwargs):
        if self.debug:
            logger.debug('Event: {0} Args: {1} Kwargs: {2}'.format(event, args, kwargs))
        for handler in self.handlers[event]:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                if event != 'event_handler_exception':
                    self.emit('event_handler_exception', event, e)
