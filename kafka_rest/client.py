import logging
from threading import Thread
from Queue import Queue, PriorityQueue, Full
from collections import defaultdict, namedtuple

from tornado.ioloop import IOLoop

from .events import EventRegistrar, DropReason
from .producer import AsyncProducer
from .message import Message
from .exceptions import KafkaRESTShutdownException

logger = logging.getLogger('kafka_rest.client')

class KafkaRESTClient(object):
    def __init__(self, host, port, http_max_clients=10, max_queue_size_per_topic=10000,
                 flush_length_threshold=20, flush_time_threshold_seconds=20,
                 flush_max_batch_size=50, connect_timeout_seconds=60,
                 request_timeout_seconds=60, retry_base_seconds=2,
                 retry_max_attempts=10, retry_period_seconds=15,
                 shutdown_timeout_seconds=2):
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
        # On shutdown, last-ditch flush attempts are given this
        # request timeout after which they are considered failed
        self.shutdown_timeout_seconds = shutdown_timeout_seconds

        self.in_shutdown = False

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
        logger.debug('Started producer background thread')

        self.registrar.emit('client.init', self)

        logger.debug('Kafka REST async client initialized for {}:{}'.format(self.host, self.port))

    def produce(self, topic, value, value_schema, key=None, key_schema=None, partition=None):
        """Place this message on the appropriate topic queue for asynchronous
        emission."""
        if self.in_shutdown:
            raise KafkaRESTShutdownException('Client is in shutdown state, new events cannot be produced')

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
            logger.debug('Storing initial value schema for topic {} in schema cache: {}'.format(topic, value_schema))
            self.schema_cache['value'][topic] = value_schema
        if key_schema and topic not in self.schema_cache['key']:
            logger.debug('Storing initial key schema for topic {} in schema cache: {}'.format(topic, key_schema))
            self.schema_cache['key'][topic] = key_schema

        queue = self.message_queues[topic]
        message = Message(topic, value, key, partition, 0, 1)
        try:
            queue.put_nowait(message)
        except Full:
            logger.critical('Primary event queue is full for topic {}, message {} will be dropped'.format(topic, message))
            self.registrar.emit('drop_message', topic, message, DropReason.PRIMARY_QUEUE_FULL)
        else:
            self.registrar.emit('produce', message)
            self.io_loop.add_callback(self.producer.evaluate_queue, topic, queue)

    def shutdown(self, block=False):
        """Prohibit further produce requests and attempt to flush all events currently in
        the main and retry queues. After this attempt, all remaining events are made
        available to an event handler but will otherwise be dropped. The producer
        thread and IOLoop are also shut down. If block=True, this blocks until
        the producer thread is dead and the shutdown event has been handled."""
        logger.info('Client shutting down')
        self.in_shutdown = True
        self.io_loop.add_callback(self.producer.start_shutdown)

        if block:
            self.producer_thread.join()
            logger.info('Client completed shutdown')
        else:
            logger.info('Client shutting down asynchronously, will not block')
