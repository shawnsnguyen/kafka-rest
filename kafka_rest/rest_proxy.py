from tornado.httpclient import HTTPRequest
from tornado.escape import json_encode

ERROR_CODES = {
    40401: 'Topic not found',
    40402: 'Partition not found',
    50001: 'Zookeeper error',
    50002: 'Non-retriable Kafka error',
    50003: 'Retriable Kafka error',
    50101: 'Only SSL endpoints found, but SSL not supported for invoked API'
}

RETRIABLE_ERROR_CODES = set([50001, 50002, 50003])

def request_for_batch(host, port, connect_timeout, request_timeout,
                      schema_cache, topic, batch):
    """Returns a Tornado HTTPRequest to the REST proxy
    representing the given message batch. This does not
    send the request, it just creates the object."""
    body = {'records': [{'value': message.value, 'key': message.key, 'partition': message.partition}
                        for message in batch]}

    # The REST proxy's API requires us to double-encode the schemas.
    # Don't ask why, because I have no idea.
    value_schema, key_schema = schema_cache['value'][topic], schema_cache['key'].get(topic)
    if isinstance(value_schema, dict):
        body['value_schema'] = json_encode(value_schema)
    else:
        body['value_schema_id'] = value_schema
    if key_schema:
        if isinstance(key_schema, dict):
            body['key_schema'] = json_encode(key_schema)
        else:
            body['key_schema_id'] = key_schema

    request = HTTPRequest('{}:{}/topics/{}'.format(host, port, topic),
                          connect_timeout=connect_timeout,
                          request_timeout=request_timeout,
                          method='POST',
                          headers={'Accept': 'application/vnd.kafka.v1+json',
                                   'Content-Type': 'application/vnd.kafka.avro.v1+json'},
                          body=json_encode(body))

    # We also stick the message batch on the HTTPRequest object itself
    # so it is available to us when we handle the response. This is necessary
    # because individual messages can fail even if the overall request is
    # successful.
    request._batch = batch
    return request
