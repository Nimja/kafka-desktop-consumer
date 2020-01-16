from confluent_kafka import Consumer, TopicPartition, KafkaError, OFFSET_BEGINNING
from . import avro_serializer
import json

TIMEOUT = 5.0

AVRO_PREFIX = b'Obj\x01\x04\x14avro.codec'


class KafkaConsumer():
    config = {}
    limit = 100
    offsets = {}

    _consumer = None

    def __init__(self, kafka_settings, limit=100):
        """
        Setup config and limit.
        """
        config = {
            'error_cb': self.error_callback,
            **kafka_settings,
            'enable.auto.commit': True,
            'auto.offset.reset': 'latest',
        }
        self.config = config
        self.limit = int(limit)

    def _get_consumer(self):
        """
        Create consumer, only once to keep the connection.
        """
        if not self._consumer:
            self._consumer = Consumer(self.config)
        return self._consumer

    def get_topic_list(self):
        """
        Get sorted list of all topics.

        This is always done as the first call and also serves as a "connect".
        """
        try:
            cluster_metadata = self._get_consumer().list_topics(timeout=TIMEOUT)
            topics = list(cluster_metadata.topics.keys())
            topics.sort()
            return topics
        except Exception as e:
            print("Error retrieving topics: ", e)
        return []

    def list_topics(self, with_messages_only=True):
        """
        Get sorted list of all topics with a message.

        Called from command-line with ./list_topics.py
        """
        if with_messages_only:
            print ("Listing topics with messages... (use any parameter to list all)")
        else:
            print ("Listing all topics...")
        consumer = self._get_consumer()
        cluster_metadata = consumer.list_topics(timeout=TIMEOUT)
        topics = list(cluster_metadata.topics.keys())
        topics.sort()
        for topic in topics:
            if with_messages_only:
                consumer.subscribe([topic])
                msg = consumer.poll(timeout=2)
                if msg is None:
                    continue
            print(topic)
        print(' - - - Done. - - - ')

    def topic_assigned(self, consumer, partitions):
        """
        Run when consumer has assigned the topic, here we can influence the offset.
        """
        for p in partitions:
            if self.offset > 0:
                p.offset = self.offset
            else:
                offset = p.offset - self.limit - 1
                if offset < 0:
                    offset = OFFSET_BEGINNING
                p.offset = offset
        consumer.assign(partitions)

    def consume(self, topic, offset=0):
        self.offset = offset
        consumer = self._get_consumer()
        # Subscribe and set offset.
        consumer.subscribe(topic, on_assign=self.topic_assigned)
        result = self._get_messages(consumer)
        return result

    def _get_messages(self, consumer):
        """ Get all the messages from the current topics."""
        count = 0
        messages = []
        eof_reached = {}
        while count < self.limit:
            msg = consumer.poll(timeout=TIMEOUT)
            if msg is None:
                break

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
