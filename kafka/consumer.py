from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING
from . import avro_serializer

TIMEOUT = 10.0


class KafkaConsumer():
    config = {}
    limit = 100

    def __init__(self, kafka_settings, limit=100):
        config = {
            'error_cb': self.error_callback,
            **kafka_settings
        }
        self.config = config
        self.limit = limit

    def topic_assigned(self, consumer, partitions):
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    def consume(self, topic):
        consumer = Consumer(self.config)
        # Subscribe to topics
        consumer.subscribe(topic, on_assign=self.topic_assigned)

        count = 0
        result = []
        eof_reached = {}
        while count < self.limit:
            msg = consumer.poll(timeout=TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_reached[(msg.topic(), msg.partition())] = True
                    if len(eof_reached) == len(consumer.assignment()):
                        # Reached end of all partitions/topics.
                        break
                else:
                    raise Exception(msg.error())
            else:
                result.append(
                    {
                        'key': msg.key().decode('utf8'),
                        'value': self._auto_decode(msg.value()),
                        'offset': msg.offset()
                    }
                )
            count += 1

        return result

    def _auto_decode(self, value):
        return avro_serializer.deserialize_first(value)

    def error_callback(self, err):
        """ Any errors in the producer will be raised here. For example if Kafka cannot connect. """
        if err is not None:
            raise Exception("Kafka Error - %s" % err)
