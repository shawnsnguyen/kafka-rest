from threading import Thread
from Queue import Queue, Full
from collections import defaultdict, namedtuple

from tornado.ioloop import IOLoop

from .events import EventRegistrar
from .producer import AsyncProducer

Message = namedtuple('Message', ['topic', 'value', 'key', 'partition'])

class KafkaRESTClient(object):
    def __init__(self, host, port, max_queue_size_per_topic=10000,
                 flush_length_threshold=20, flush_time_threshold_seconds=20,
                 flush_max_batch_size=50, connect_timeout_seconds=60,
                 request_timeout_seconds=60):
        self.host = host
        self.port = port
        self.max_queue_size_per_topic = max_queue_size_per_topic
        self.flush_length_threshold = flush_length_threshold
        self.flush_time_threshold_seconds = flush_time_threshold_seconds
        self.flush_max_batch_size = flush_max_batch_size
        self.connect_timeout_seconds = connect_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds

        self.registrar = EventRegistrar()
        self.message_queues = defaultdict(lambda: Queue(maxsize=max_queue_size_per_topic))
        # Schema cache is carefully accessed by both threads, make sure to read the detailed
        # comments about it where it's used before changing anything
        self.schema_cache = {'value': {}, 'key': {}}
        self.io_loop = IOLoop()

        self.producer = AsyncProducer(self)

        self.producer_thread = Thread(target=self.io_loop.start)
        self.producer_thread.daemon = True
        self.producer_thread.start()

    def produce(self, topic, value, value_schema, key=None, key_schema=None, partition=None):
        """Place this message on the appropriate topic queue for asynchronous
        emission."""
        # This piece is a bit clever. Because we do not do anything to seed the schemas
        # for a topic before the first produce call, we know that the main thread will be the
        # first to touch the schema cache key for a given topic. Therefore, if and only if the key
        # is not present, it is safe for the main thread to write the full schema value to the cache.
        # After this initial write, only the producer thread should overwrite this key,
        # and it will only do that to replace the full schema with the ID it gets back
        # from its first request using this schema. This allows us to avoid queuing the schema with all
        # messages and should still be consistent and avoid concurrency issues.
        # N.B. This all assumes the schema for a topic does not change during a process's lifetime.
        if topic not in self.schema_cache['value']:
            self.schema_cache['value'][topic] = value_schema
        if key_schema and topic not in self.schema_cache['key']:
            self.schema_cache['key'][topic] = key_schema

        queue = self.message_queues[topic]
        try:
            queue.put_nowait(Message(topic, value, key, partition))
        except Full:
            pass
        self.io_loop.add_callback(self.producer.evaluate_queue, topic, queue)
