import sys
from threading import Thread
try:
    from queue import Queue, PriorityQueue, Full
except ImportError:
    from Queue import Queue, PriorityQueue, Full
from collections import defaultdict, namedtuple

from tornado.ioloop import IOLoop

from .events import EventRegistrar, DropReason
from .circuit_breaker import CircuitBreaker
from .producer import AsyncProducer
from .message import Message
from .exceptions import KafkaRESTShutdownException
from .custom_logging import getLogger

logger = getLogger('kafka_rest.client')

class KafkaRESTClient(object):
    def __init__(self, host, port, http_max_clients=10, max_queue_size_per_topic=5000,
                 flush_length_threshold=20, flush_time_threshold_seconds=20,
                 flush_max_batch_size=50, connect_timeout_seconds=10,
                 request_timeout_seconds=60, retry_base_seconds=2,
                 retry_max_attempts=10, retry_period_seconds=15,
                 response_5xx_circuit_breaker_trip_threshold=10,
                 response_5xx_circuit_breaker_trip_duration_seconds=300,
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
        # Circuit breaker prevents thrashing if we receive multiple transport
        # errors in a row, since this usually means we are experiencing some
        # sort of network problem and other requests are very likely to fail.
        # When this triggers, we wait a short duration before clearing the
        # breaker and attempting any further network operations.
        if response_5xx_circuit_breaker_trip_threshold is None:
            response_5xx_circuit_breaker_trip_threshold = sys.maxsize
        self.response_5xx_circuit_breaker_trip_threshold = response_5xx_circuit_breaker_trip_threshold
        self.response_5xx_circuit_breaker_trip_duration_seconds = response_5xx_circuit_breaker_trip_duration_seconds
        # On shutdown, last-ditch flush attempts are given this
        # request timeout after which they are considered failed
        self.shutdown_timeout_seconds = shutdown_timeout_seconds

        self.in_shutdown = False

        self.registrar = EventRegistrar()
        self.response_5xx_circuit_breaker = CircuitBreaker(self.response_5xx_circuit_breaker_trip_threshold,
                                                           self.response_5xx_circuit_breaker_trip_duration_seconds)
        self.message_queues = defaultdict(lambda: Queue(maxsize=max_queue_size_per_topic))
        self.retry_queues = defaultdict(lambda: PriorityQueue(maxsize=max_queue_size_per_topic))
        self.schema_cache = defaultdict(dict)
        self.io_loop = IOLoop()

        self.producer = AsyncProducer(self)
        self.io_loop.add_callback(self.producer._schedule_retry_periodically)

        self.producer_thread = Thread(target=self.io_loop.start)
        self.producer_thread.daemon = True
        self.producer_thread.start()
        logger.debug('Started producer background thread')

        logger.debug('Kafka REST async client initialized for {0}:{1}'.format(self.host, self.port))

    def produce(self, topic, value, value_schema, key=None, key_schema=None, partition=None):
        """Place this message on the appropriate topic queue for asynchronous
        emission."""
        if self.in_shutdown:
            raise KafkaRESTShutdownException('Client is in shutdown state, new events cannot be produced')

        if self.schema_cache[topic].get('value') is None:
            logger.trace('Storing initial value schema for topic {0} in schema cache: {1}'.format(topic, value_schema))
            self.schema_cache[topic]['value'] = value_schema
        if key_schema and self.schema_cache[topic].get('key') is None:
            logger.trace('Storing initial key schema for topic {0} in schema cache: {1}'.format(topic, key_schema))
            self.schema_cache[topic]['key'] = key_schema

        queue = self.message_queues[topic]
        message = Message(topic, value, key, partition, 0, 1)
        try:
            queue.put_nowait(message)
        except Full:
            logger.critical('Primary event queue is full for topic {0}, message {1} will be dropped'.format(topic, message))
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
