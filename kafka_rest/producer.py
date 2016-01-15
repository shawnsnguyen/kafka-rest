import json
from functools import partial

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.escape import json_decode

from .rest_proxy import request_for_batch

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
        print len(batch)
        self.http_client.fetch(request,
                               callback=partial(self._handle_produce_response, topic),
                               raise_error=False)

    def _handle_produce_response(self, topic, response):
        if response.code == 200:
            response_body = json_decode(response.body)
            # Store schema IDs if we haven't already
            if not isinstance(self.client.schema_cache['value'][topic], int):
                self.client.schema_cache['value'][topic] = response_body['value_schema_id']
            if not isinstance(self.client.schema_cache['key'].get(topic), int):
                self.client.schema_cache['key'][topic] = response_body['key_schema_id']

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
