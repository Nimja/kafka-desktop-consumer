import json
from .schema_registry_client import AvroSchemaRegistryClient
from .embedded_serializer import EmbeddedSerializer
from .registry_serializer import RegistrySerializer

AVRO_SCHEMA_PREFIX = b'Obj\x01\x04\x14avro.codec'
AVRO_REPOSITORY_PREFIX = b'\x00'


class AutoDecode:  # pylint: disable=too-few-public-methods
    """
    Automatically decode a kafka bytes message into python dict, using embedded or schema registry.
    """

    def __init__(self, registry_client: AvroSchemaRegistryClient) -> None:
        self.embedded_serializer = EmbeddedSerializer()
        self.registry_serializer = RegistrySerializer(registry_client)

    def decode(self, message: bytes):
        """ Decode a single message. """
        # Embedded avro message.
        if message[:len(AVRO_SCHEMA_PREFIX)] == AVRO_SCHEMA_PREFIX:
            return self.embedded_serializer.deserialize(message)
        # Schema registry avro message.
        if message[:len(AVRO_REPOSITORY_PREFIX)] == AVRO_REPOSITORY_PREFIX:
            return self.registry_serializer.deserialize(message)

        # PROBABLY plain JSON.
        decoded = message.decode("utf-8")
        try:
            return json.loads(decoded)
        except json.JSONDecodeError:
            return decoded
