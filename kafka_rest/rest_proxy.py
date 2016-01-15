from tornado.httpclient import HTTPRequest
from tornado.escape import json_encode

def request_for_batch(host, port, connect_timeout, request_timeout,
                      schema_cache, topic, batch):
    """Returns a Tornado HTTPRequest to the REST proxy
    representing the given message batch. This does not
    send the request, it just creates the object."""
    body = {'records': [{'value': message.value, 'key': message.key, 'partition': message.partition}
                        for message in batch]}

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

    return HTTPRequest('{}:{}/topics/{}'.format(host, port, topic),
                       connect_timeout=connect_timeout,
                       request_timeout=request_timeout,
                       method='POST',
                       headers={'Accept': 'application/vnd.kafka.v1+json',
                                'Content-Type': 'application/vnd.kafka.avro.v1+json'},
                       body=json_encode(body))
