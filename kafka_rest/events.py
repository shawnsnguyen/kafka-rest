import logging
from collections import defaultdict

logger = logging.getLogger('kafka_rest.events')

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
            logger.debug('Event: {} Args: {} Kwargs: {}'.format(event, args, kwargs))
        for handler in self.handlers[event]:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                if event != 'event_handler_exception':
                    self.emit('event_handler_exception', event, e)
