from confluent_kafka import Consumer, TopicPartition, KafkaError, OFFSET_BEGINNING
from .avro_schemas.auto_decode import AutoDecode
from .avro_schemas.schema_registry_client import AvroSchemaRegistryClient
import io

TIMEOUT = 5.0


class KafkaConsumer():
    config = {}
    limit = 100

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

    def consume(self, topic, offset=0):
        consumer = self._get_consumer()
        if offset <= 0:
            offset = OFFSET_BEGINNING
        # Subscribe and set offset.
        consumer.assign([TopicPartition(topic, 0, offset)])

        count = 0
        eof_reached = {}
        while count < self.limit:
            msg = consumer.poll(timeout=TIMEOUT)
            if msg is None:  # Nothing to read.
                break

            if msg.error(): # Message with error, which could just be end of partition.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_reached[(msg.topic(), msg.partition())] = True
                    if len(eof_reached) == len(consumer.assignment()):
                        # Reached end of all partitions/topics.
                        break
                else:
                    raise Exception(msg.error())
            else: # A normal message.
                yield(
                    {
                        'key':  self.auto_decode.decode(msg.key()),
                        'value': self.auto_decode.decode(msg.value()),
                        'offset': msg.offset()
                    }
                )
            count += 1

    def error_callback(self, err):
        """ Any errors in the producer will be raised here. For example if Kafka cannot connect. """
        if err is not None:
            raise Exception("Kafka Error - %s" % err)
