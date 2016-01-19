import json
from functools import partial
import time
from Queue import Full
from collections import namedtuple

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.escape import json_decode

from .rest_proxy import request_for_batch, RETRIABLE_ERROR_CODES
from .message import Message

class FlushReason(object):
    LENGTH = 'length'
    TIME = 'time'
    QUIT = 'quit'

class AsyncProducer(object):
    def __init__(self, client):
        self.client = client
        # References to timed callbacks in the event loop
        self.timers = {}
        self.http_client = AsyncHTTPClient(io_loop=self.client.io_loop)

    def _reset_timer(self, topic):
        if topic in self.timers:
            IOLoop.current().remove_timeout(self.timers[topic])
        handle = IOLoop.current().call_later(self.client.flush_time_threshold_seconds,
                                             self._flush_topic, topic, FlushReason.TIME)
        self.timers[topic] = handle

    def _send_batch_produce_request(self, topic, batch):
        request = request_for_batch(self.client.host, self.client.port,
                                    self.client.connect_timeout_seconds,
                                    self.client.request_timeout_seconds,
                                    self.client.schema_cache, topic, batch)
        self.http_client.fetch(request,
                               callback=partial(self._handle_produce_response, topic),
                               raise_error=False)

    def _queue_message_for_retry(self, topic, message):
        if message.can_retry(self.client):
            try:
                self.client.retry_queues[topic].put_nowait(message.for_retry(self.client))
            except Full:
                pass
        else:
            pass

    def _handle_produce_success(self, topic, response, response_body):
        # Store schema IDs if we haven't already
        if not isinstance(self.client.schema_cache['value'][topic], int):
            self.client.schema_cache['value'][topic] = response_body['value_schema_id']
        if not isinstance(self.client.schema_cache['key'].get(topic), int):
            self.client.schema_cache['key'][topic] = response_body['key_schema_id']

        # Individual requests could still have failed, need to check
        # each response object's error code
        for idx, offset in enumerate(response_body['offsets']):
            if offset.get('error_code') == 1: # Non-retriable Kafka exception
                pass
            elif offset.get('error_code') == 2: # Retriable Kafka exception
                message = response.request._batch[idx]
                self._queue_message_for_retry(topic, message)

    def _handle_produce_response(self, topic, response):
        # First, we check for a transport error
        if response.error:
            return

        # We should have gotten a well-formed response back from the
        # proxy if we got this far
        response_body = json_decode(response.body)
        error_code, error_message = response_body.get('error_code'), response_body.get('message')

        if response.code == 200:
            self._handle_produce_success(topic, response, response_body)
        else: # We failed somehow, more information in the error code
            if error_code in RETRIABLE_ERROR_CODES:
                for message in response.request._batch:
                    self._queue_message_for_retry(topic, message)
            else: # Non-retriable failure of entire request
                pass

    def _flush_topic(self, topic, reason):
        queue, batch = self.client.message_queues[topic], []
        while not queue.empty():
            message = queue.get_nowait()
            if message:
                batch.append(message)
            else:
                break
            if len(batch) >= self.client.flush_max_batch_size:
                IOLoop.current().add_callback(self._send_batch_produce_request, topic, batch)
                batch = []
        if batch:
            IOLoop.current().add_callback(self._send_batch_produce_request, topic, batch)
        self._reset_timer(topic)

    def evaluate_queue(self, topic, queue):
        if queue.qsize() >= self.client.flush_length_threshold:
            self._flush_topic(topic, FlushReason.LENGTH)
        elif topic not in self.timers:
            self._reset_timer(topic)
