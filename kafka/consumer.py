from confluent_kafka import Consumer, TopicPartition, KafkaError, OFFSET_BEGINNING
from .avro_schemas.auto_decode import AutoDecode
from .avro_schemas.schema_registry_client import AvroSchemaRegistryClient
import io

TIMEOUT = 5.0

class KafkaConsumer():
    config = {}
    limit = 100
    offsets = {}

    _consumer = None

    def __init__(self, kafka_settings, avro_settings, limit=100):
        """
        Setup config and limit.
        """
        self.auto_decode = AutoDecode(
            AvroSchemaRegistryClient(avro_settings.get('registry.url', ''))
        )
        config = {
            'error_cb': self.error_callback,
            **kafka_settings,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest',
            'default.topic.config': {'auto.offset.reset': 'earliest'}
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

    def consume(self, topic, offset=0):
        self.offset = offset
        consumer = self._get_consumer()
        # Subscribe and set offset.
        consumer.assign([TopicPartition(topic[0], 0, offset)])
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
                        'value': self.auto_decode.decode(msg.value()),
                        'offset': msg.offset()
                    }
                )
            count += 1
        return messages

    def error_callback(self, err):
        """ Any errors in the producer will be raised here. For example if Kafka cannot connect. """
        if err is not None:
            raise Exception("Kafka Error - %s" % err)
