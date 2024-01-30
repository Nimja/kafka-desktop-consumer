# Standard Library Imports
import io
import struct

# Third Party Library Imports
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder, AvroTypeException
from avro_validator.schema import Schema as SchemaValidator
from .schema_registry_client import AvroSchemaRegistryClient
from ..exception import AvroSchemaException


MAGIC_BYTE = 0


class BytesIOContext(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class RegistrySerializer:
    """
    Serialize avro messages with schema registry.
    """

    def __init__(self, client: AvroSchemaRegistryClient) -> None:
        self.client = client
        self.id_to_readers = {}
        self.id_to_writers = {}

    def _get_reader_for_schema_id(self, schema_id) -> DatumWriter:
        """ Get reader for specific schema ID. """
        if schema_id not in self.id_to_readers:
            schema = self.client.get_by_schema_id(schema_id)
            if not schema:
                raise AvroSchemaException("Schema does not exist")

            self.id_to_readers[schema_id] = DatumReader(schema)
        return self.id_to_readers[schema_id]

    def deserialize(self, message: bytes) -> dict:
        """ Deserialize a single message. """
        with BytesIOContext(message) as payload:
            magic_byte, schema_id = struct.unpack('>bI', payload.read(5))
            if magic_byte is not MAGIC_BYTE:
                raise AvroSchemaException("Magic byte missing?")
            avro_reader = self._get_reader_for_schema_id(schema_id)
            return avro_reader.read(BinaryDecoder(payload))

    def _get_writer_for_schema_id(self, schema_id) -> DatumWriter:
        """ Get writer for specific schema ID. """
        if schema_id not in self.id_to_writers:
            schema = self.client.get_by_schema_id(schema_id)
            if not schema:
                raise AvroSchemaException("Schema does not exist")

            self.id_to_writers[schema_id] = DatumWriter(schema)
        return self.id_to_writers[schema_id]

    def serialize(self, record: dict, schema_id: int) -> bytes:
        """
        Serialize a single message with embedded AVRO schema by ID.

        On encoding exception, use the much nicer library to get a more helpful error!
        """
        try:
            return self._serialize(record, schema_id)
        except AvroTypeException:
            schema = SchemaValidator(self.client.get_json_by_schema_id(schema_id))
            parsed_schema = schema.parse()
            parsed_schema.validate(record)
            raise  # If the validator did not raise an error, raise again.

    def _serialize(self, record: dict, schema_id: int) -> bytes:
        """ Serialize a single message with embedded AVRO schema by ID. """
        # get the writer
        writer = self._get_writer_for_schema_id(schema_id)
        with BytesIOContext() as outf:
            # Write the header: Magic byte
            outf.write(struct.pack('b', MAGIC_BYTE))
            # Write the schema ID in network byte order (big end)
            outf.write(struct.pack('>I', schema_id))

            # --- Rest --- Write the record.
            # Create an encoder that we'll write to
            encoder = BinaryEncoder(outf)
            # Write the object in 'obj' as Avro to the fake file...
            writer.write(record, encoder)
            # Return the bytes.
            return outf.getvalue()
