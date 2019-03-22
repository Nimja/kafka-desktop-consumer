from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING
from . import avro_serializer
import json

TIMEOUT = 10.0

AVRO_PREFIX = b'Obj\x01\x04\x14avro.codec'


class KafkaConsumer():
    config = {}
    limit = 100
    offsets = {}

    def __init__(self, kafka_settings, limit=100):
        config = {
            'error_cb': self.error_callback,
            **kafka_settings,
            'auto.offset.reset': 'earliest',
        }
        self.config = config
        self.limit = int(limit)

    def topic_assigned(self, consumer, partitions):
        for p in partitions:
            offset = self.offsets.get((p.topic, p.partition)) - self.limit - 1
            if offset < 0:
                offset = OFFSET_BEGINNING
            p.offset = offset
        consumer.assign(partitions)

    def consume(self, topic):
        consumer = Consumer(self.config)
        # Subscribe to topics
        consumer.subscribe(topic)
        self._get_messages(consumer)
        consumer.subscribe(topic, on_assign=self.topic_assigned)
        return self._get_messages(consumer)

    def _get_messages(self, consumer):
        """ Get all the messages from the current topics."""
        count = 0
        messages = []
        eof_reached = {}
        while count < self.limit:
            msg = consumer.poll(timeout=TIMEOUT)
            if msg is None:
                continue

            self.offsets[(msg.topic(), msg.partition())] = msg.offset()
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_reached[(msg.topic(), msg.partition())] = True
                    if len(eof_reached) == len(consumer.assignment()):
                        # Reached end of all partitions/topics.
                        break
                else:
                    raise Exception(msg.error())
            else:
                messages.append(
                    {
                        'key': msg.key().decode('utf8'),
                        'value': self._auto_decode(msg.value()),
                        'offset': msg.offset()
                    }
                )
            count += 1
        return messages

    def _auto_decode(self, value):
        """
        If it starts with AVRO prefix, we decode it.

        Otherwise we attempt JSON or just return text.
        :param value:
        :return:
        """
        if value[:len(AVRO_PREFIX)] == AVRO_PREFIX:
            return avro_serializer.deserialize_first(value)

        decoded = value.decode("utf-8")
        try:
            return json.loads(decoded)
        except Exception:
            return decoded


    def error_callback(self, err):
        """ Any errors in the producer will be raised here. For example if Kafka cannot connect. """
        if err is not None:
            raise Exception("Kafka Error - %s" % err)
