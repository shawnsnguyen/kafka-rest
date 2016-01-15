from collections import defaultdict

class EventRegistrar(object):
    """
    Handles registration of callable event handlers which are
    executed in sequence when events are emitted.
    """
    def __init__(self):
        self.handlers = defaultdict(list)

    def register(self, event, handler):
        self.handlers[event].append(handler)

    def emit(self, event, *args, **kwargs):
        for handler in self.handlers[event]:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                if event != 'event_handler_exception':
                    self.emit('event_handler_exception', event, e)
