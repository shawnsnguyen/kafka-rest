from threading import Thread
from Queue import Queue, PriorityQueue, Full
from collections import defaultdict, namedtuple

from tornado.ioloop import IOLoop

from .events import EventRegistrar
from .producer import AsyncProducer
from .message import Message

class KafkaRESTClient(object):
    def __init__(self, host, port, http_max_clients=10, max_queue_size_per_topic=10000,
                 flush_length_threshold=20, flush_time_threshold_seconds=20,
                 flush_max_batch_size=50, connect_timeout_seconds=60,
                 request_timeout_seconds=60, retry_base_seconds=2,
                 retry_max_attempts=10, retry_period_seconds=15):
        self.host = host
        self.port = port
        self.http_max_clients = http_max_clients
        self.max_queue_size_per_topic = max_queue_size_per_topic
        self.flush_length_threshold = flush_length_threshold
        self.flush_time_threshold_seconds = flush_time_threshold_seconds
        self.flush_max_batch_size = flush_max_batch_size
        self.connect_timeout_seconds = connect_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.retry_base_seconds = retry_base_seconds
        # Includes the original send as an attempt, so set to 1 to disable retry
        self.retry_max_attempts = retry_max_attempts
        self.retry_period_seconds = retry_period_seconds

        self.registrar = EventRegistrar()
        self.message_queues = defaultdict(lambda: Queue(maxsize=max_queue_size_per_topic))
        self.retry_queues = defaultdict(lambda: PriorityQueue(maxsize=max_queue_size_per_topic))
        # Schema cache is carefully accessed by both threads, make sure to read the detailed
        # comments about it where it's used before changing anything
        self.schema_cache = {'value': {}, 'key': {}}
        self.io_loop = IOLoop()

        self.producer = AsyncProducer(self)
        self.io_loop.add_callback(self.producer._schedule_retry_periodically)

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
            queue.put_nowait(Message(topic, value, key, partition, 0, 1))
        except Full:
            pass
        self.io_loop.add_callback(self.producer.evaluate_queue, topic, queue)
