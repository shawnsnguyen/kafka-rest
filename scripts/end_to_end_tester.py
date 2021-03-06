"""Since this library doesn't have a legit consumer implementation yet,
we fall back to good ol' requests to stream the results out of the
REST proxy.

Usage: python end_to_end_tester.py <rest proxy host> <rest proxy port> <topic_name_for_testing>"""

import sys
import json
import logging
import uuid
from collections import defaultdict
import time
import signal

import requests
from tornado.ioloop import PeriodicCallback

from kafka_rest.client import KafkaRESTClient

CONSUMER_GROUP_NAME = uuid.uuid4()
TEST_SCHEMA = {
    'type': 'record',
    'name': 'test_driver',
    'fields': [
        {'name': 'val', 'type': 'int'}
    ]
}
NUM_EVENTS_PER_BATCH = 20
SEND_INTERVAL_SECONDS = 5
latest_event_num = 0

# nonlocal keyword would be nice
_first_interrupt, _stop = True, False

RESULT_FILE = 'end_to_end_test.json'
LOG_FILE = 'end_to_end_test.log'

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)

def _produce_events(client, topic):
    global latest_event_num
    for i in range(NUM_EVENTS_PER_BATCH):
        client.produce(topic, {'val': latest_event_num}, TEST_SCHEMA)
        latest_event_num += 1

def _produce_events_forever(client, topic):
    cb = PeriodicCallback(lambda: _produce_events(client, topic), SEND_INTERVAL_SECONDS*1000, io_loop=client.io_loop)
    cb.start()
    return cb

def _register_consumer_instance(host, port, topic):
    payload = {'format': 'avro', 'auto.offset.reset': 'largest', 'auto.commit.enable': True}
    r = requests.post('{}:{}/consumers/{}'.format(host, port, CONSUMER_GROUP_NAME),
                      json=payload)
    r.raise_for_status()
    j = r.json()
    return j['instance_id'], j['base_uri']

def _set_initial_offset(base_uri, topic):
    r = requests.get('{}/topics/{}'.format(base_uri, topic),
                     headers={'Accept': 'application/vnd.kafka.avro.v1+json'},
                     params={'max_bytes': 1024})
    r.raise_for_status()

def _consume_events(client, produce_cb, base_uri, topic):
    seen = defaultdict(int)
    print 'Starting to consume events'
    print 'Ctrl-C once to stop producing'
    print 'Ctrl-C again to stop consuming'

    def signal_handler(signal, frame):
        global _first_interrupt, _stop
        if _first_interrupt:
            print 'Stopping production of new events'
            produce_cb.stop()
            _first_interrupt = False
        else:
            print 'Stopping consumer'
            _stop = True
    signal.signal(signal.SIGINT, signal_handler)

    while not _stop:
        r = requests.get('{}/topics/{}'.format(base_uri, topic),
                         headers={'Accept': 'application/vnd.kafka.avro.v1+json'},
                         params={'max_bytes': 1024*50})
        try:
            r.raise_for_status()
        except Exception:
            print 'Got error response from proxy, waiting 10 seconds to continue'
            time.sleep(10)
            continue
        this_batch = 0
        for message in r.json():
            this_batch += 1
            seen[message['value']['val']] += 1
        print 'Got {} messages'.format(this_batch)
        print '{} produced / {} consumed'.format(latest_event_num, len(seen.keys()))

    return seen

def _delete_consumer(base_uri):
    r = requests.delete(base_uri)
    r.raise_for_status()

def _write_results(seen):
    exact, missing, extra = 0, 0, 0
    for i in range(latest_event_num):
        if seen[i] == 0:
            missing += 1
        elif seen[i] == 1:
            exact += 1
        else:
            extra += 1

    data = {'summary': {'num_events': latest_event_num,
                        'exact': exact,
                        'missing': missing,
                        'extra': extra},
            'detail': seen}
    print data['summary']
    with open(RESULT_FILE, 'w') as f:
        f.write(json.dumps(data))

def main(host, port, topic):
    client = KafkaRESTClient(host, port, response_5xx_circuit_breaker_trip_duration_seconds=30)
    client.registrar.debug = True

    # Set up consumer before emitting so we start with an
    # offset before any of our new events hit
    print 'Registering consumer group with initial offset'
    instance_id, base_uri = _register_consumer_instance(host, port, topic)
    _set_initial_offset(base_uri, topic)
    raw_input('Consumer offset initialized, press ENTER to continue\n')

    print 'Producing events asynchronously'
    cb = _produce_events_forever(client, topic)

    events_seen = _consume_events(client, cb, base_uri, topic)

    print 'Writing results to {}'.format(RESULT_FILE)
    _write_results(events_seen)

    print 'Cleaning up consumer'
    _delete_consumer(base_uri)

    client.shutdown(block=True)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
