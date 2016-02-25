# kafka-rest
Async Python client for [Confluent's Kafka REST proxy](http://docs.confluent.io/1.0/kafka-rest/docs/intro.html)

## Table of Contents

- [Quick Start](#quick-start)
- [Concurrency Model](#architecture)
  - [Thread and Fork Safety](#thread-and-fork-safety)
- [Message Sending Lifecycle](#message-sending-lifecycle)
- [Shutdown Flow](#shutdown-flow)
- [Circuit Breakers](#circuit-breakers)
- [Event Hooks](#event-hooks)

## Quick Start

```python
from kafka_rest.client import KafkaRESTClient

# Initialize a client. Upon initialization, the client starts
# a new thread which runs a Tornado IOLoop
client = KafkaRESTClient(host='http://kafka-rest.mydomain.com', port=8082)

avro_value_schema = {'type': 'record', 'name': 'test', 'fields': [{'name': val', 'type': 'string'}]}
avro_value = {'val': 'my fun avro value'}

# The client's produce method queues a message to be sent later
# in the client's IOLoop thread. This produce call itself does
# very little other than put this message in an internal queue
client.produce('topic_name', avro_value, avro_schema)

# rest of program goes here...

# The client batches messages together and does not always
# send immediately after produce is called. To make sure we
# flush any remaining messages when our program exits, we
# call the client's shutdown method
client.shutdown(block=True)
```

## Concurrency Model

Upon instantiation, the `KafkaRESTClient` starts a new thread which
runs a [Tornado](https://github.com/tornadoweb/tornado) `IOLoop`. Almost all of the
work the client does is performed in this new thread. This thread and the `IOLoop`
within it can be terminated by calling the client's `shutdown` method.

The `IOLoop` manages internal queues for storing the events to be sent to the
Kafka REST proxy and is wholly responsible for communicating with the REST proxy.
All HTTP calls to the proxy are performed using non-blocking sockets.

#### Thread and Fork Safety

The client is fully thread safe. Since each client creates its own thread,
it is recommended that each Python process shares a single client across
all of its threads.

Since the client starts a new thread when it is instantiated, it may not be safe
for use in programs which fork. If your program forks, you must take care
not to instantiate a KafkaRESTClient instance until after you have forked.

## Message Sending Lifecycle

1. A message is queued to be sent via the `produce` call. This puts the message
into a per-topic queue. These queues have a maximum size. If the topic's queue
is full, the message is immediately dropped.

2. If the number of messages queued for this topic exceeds a certain threshold,
the client will immediately send the messages in the queue. If this length
threshold is not hit, the client will send any messages built up after a
certain number of seconds.

3. The client splits any messages waiting to be sent into batches and sends
each batch to the REST proxy using non-blocking sockets.

4. If the messages are successfully acknowledged by the proxy, there is nothing
else left to do. If the entire batch fails (5xx response or a timeout) or individual
messages inside the batch return error codes, the messages which failed are
placed in a retry queue. The retry queue has a maximum size, and messages
are dropped here if the queue is full.

5. Messages in the retry queue will be retried using exponential backoff, up to
a maximum number of retries. The client periodically checks for any messages
needing to be retried and sends them to the proxy in batches. If a message has
exceeded the maximum number of retries and still has not succeeded, it will
be dropped.

The queue sizes, timers, and thresholds mentioned above are all configurable
through the `KafkaRESTClient` constructor.

## Shutdown Flow

Your application must take care to shut down the client properly to ensure that
all enqueued messages are sent to the REST proxy. The recommended way to do
this is to add this to your application:

```python
import atexit
atexit.register(client.shutdown, block=True)
```

During shutdown, the following happens:

1. The client is immediately put into a shutdown state. Once in this state,
any calls to the client's `produce` method will raise an exception.

2. All remaining messages in the client's primary and retry queues are
sent to the REST proxy. They are given a number of seconds to return,
configured via `shutdown_timeout_seconds` in the client's constructor. By default,
this is 2 seconds. Any messages that fail during this time are not retried.

3. After `shutdown_timeout_seconds` has elapsed, the `IOLoop` and the client's
thread are terminated.

## Circuit Breakers

The client implements a basic [circuit breaker](http://martinfowler.com/bliki/CircuitBreaker.html)
pattern to prevent the client from overwhelming its Python process by retrying
many messages which are likely to immediately fail again.

If the client receives
many 5xx responses or timeouts from its REST proxy in a row, it will stop
trying to send new messages for a period of time (default 5 minutes). If the
client is shut down while the circuit breaker is active, it will still make a
last-ditch attempt to send any enqueued messages.

The circuit breaker behavior is tunable via the `KafkaRESTClient` constructor.

## Event Hooks

The client provides event handlers to communicate its status to other threads.
These hooks can be used to implement logging, instrumentation, saving dropped
messages to a secondary storage medium, etc.

To register an event handler, provide a callable referencing the hook to
the client's `registrar`:

```python
client = KafkaRESTClient(...)

def _my_drop_message_handler(*args, **kwargs):
    print 'This gets called whenever the client drops a message'

client.registrar.register('drop_message', _my_drop_message_handler)
```

Event handlers are executed on the client's thread, so be sure not to block
the `IOLoop` for too long.

TODO: Enumerate these in a separate Hooks.md file