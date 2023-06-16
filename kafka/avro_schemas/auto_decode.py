import json
from .schema_registry_client import AvroSchemaRegistryClient
from .embedded_serializer import EmbeddedSerializer
from .registry_serializer import RegistrySerializer

AVRO_SCHEMA_PREFIX = b'Obj\x01\x04'
AVRO_REPOSITORY_PREFIX = b'\x00'

# Possible decoding types.
DECODING_TYPE_NONE = 'none'
DECODING_TYPE_AVRO_EMBEDDED = 'avro_embedded'
DECODING_TYPE_AVRO_SCHEMA_REGISTRY = 'avro_schema_registry'
DECODING_TYPE_JSON = 'json'
DECODING_TYPE_PLAIN = 'plain'


class AutoDecode:  # pylint: disable=too-few-public-methods
    """
    Automatically decode a kafka bytes message into python dict, using embedded or schema registry.
    """

    def __init__(self, registry_client: AvroSchemaRegistryClient) -> None:
        self.embedded_serializer = EmbeddedSerializer()
        self.registry_serializer = RegistrySerializer(registry_client)

    def decode(self, message: bytes):
        """ Decode a single message. """
        if not message:
            return message, DECODING_TYPE_NONE

        # Embedded avro message.
        if message[:len(AVRO_SCHEMA_PREFIX)] == AVRO_SCHEMA_PREFIX:
            return self.embedded_serializer.deserialize(message), DECODING_TYPE_AVRO_EMBEDDED
        # Schema registry avro message.
        if message[:len(AVRO_REPOSITORY_PREFIX)] == AVRO_REPOSITORY_PREFIX:
            return self.registry_serializer.deserialize(message), DECODING_TYPE_AVRO_SCHEMA_REGISTRY

        # PROBABLY plain JSON.
        decoded = message
        try:
            decoded = message.decode("utf-8")
            return json.loads(decoded), DECODING_TYPE_JSON
        except Exception:  # pylint: disable=broad-except
            return decoded, DECODING_TYPE_PLAIN
