from tornado.httpclient import HTTPRequest
from tornado.escape import json_encode
import avro.schema
from avro_json_serializer import AvroJsonSerializer

ERROR_CODES = {
    40401: 'Topic not found',
    40402: 'Partition not found',
    50001: 'Zookeeper error',
    50002: 'Kafka error',
    50003: 'Retriable Kafka error',
    50101: 'Only SSL endpoints found, but SSL not supported for invoked API'
}

# Confluent didn't seem very confident that 50002 would
# always be non-retriable when I asked them about it,
# so we'll keep it as retriable for now to be safe.
# See https://github.com/confluentinc/kafka-rest/issues/147
RETRIABLE_ERROR_CODES = set([50001, 50002, 50003])

def _encode_payload(schema_cache, topic, batch):
    value_schema = avro.schema.make_avsc_object(schema_cache[topic]['value'], avro.schema.Names())
    value_serializer = AvroJsonSerializer(value_schema)
    if schema_cache[topic].get('key') is not None:
        key_schema = avro.schema.make_avsc_object(schema_cache[topic]['key'], avro.schema.Names())
        key_serializer = AvroJsonSerializer(key_schema)

    body = {'records': [{'value': value_serializer.to_ordered_dict(message.value),
                         'key': key_serializer.to_ordered_dict(message.key) if message.key is not None else None,
                         'partition': message.partition}
                        for message in batch]}

    # The REST proxy's API requires us to double-encode the schemas.
    # Don't ask why, because I have no idea.
    if schema_cache[topic].get('value-id') is None:
        body['value_schema'] = json_encode(schema_cache[topic]['value'])
    else:
        body['value_schema_id'] = schema_cache[topic]['value-id']
    if schema_cache[topic].get('key') is not None:
        if schema_cache[topic].get('key-id') is None:
            body['key_schema'] = json_encode(schema_cache[topic]['key'])
        else:
            body['key_schema_id'] = schema_cache[topic]['key-id']

    return json_encode(body)

def request_for_batch(host, port, connect_timeout, request_timeout,
                      schema_cache, topic, batch):
    """Returns a Tornado HTTPRequest to the REST proxy
    representing the given message batch. This does not
    send the request, it just creates the object."""
    request = HTTPRequest('{0}:{1}/topics/{2}'.format(host, port, topic),
                          connect_timeout=connect_timeout,
                          request_timeout=request_timeout,
                          method='POST',
                          headers={'Accept': 'application/vnd.kafka.v1+json',
                                   'Content-Type': 'application/vnd.kafka.avro.v1+json'},
                          body=_encode_payload(schema_cache, topic, batch))

    # We also stick the message batch on the HTTPRequest object itself
    # so it is available to us when we handle the response. This is necessary
    # because individual messages can fail even if the overall request is
    # successful.
    request._batch = batch
    return request
