# Standard Library Imports
import io

# Third Party Library Imports
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import avro.schema


class EmbeddedSerializer:  # pylint: disable=duplicate-code
    """
    Serialize avro messages with embedded schemas.
    """

    @staticmethod
    def _deserialize_list(value: bytes):
        """Deserialize AVRO encoded binary string and yield records.
        Args:
            value (str): binary string value.
        Yields:
            dict: deserialized record.
        """
        with DataFileReader(io.BytesIO(value), DatumReader()) as reader:
            for record in reader:
                yield record

    def deserialize(self, value: bytes) -> dict:
        """Deserialize AVRO encoded binary string and return the first record.
        Args:
            value (str): binary string value.
        Returns:
            dict: deserialized record.
        """
        return next(self._deserialize_list(value))

    @staticmethod
    def _serialize_list(records: list, schema_json: str) -> bytes:
        """Serialize list of records to AVRO encoded binary string.
        Args:
            records (list): list of records.
            schema_json (str): json encoded schema to be used.
        Returns:
            string: binary string value.
        """
        schema = avro.schema.parse(schema_json)  # need to know the schema to write
        output = io.BytesIO()
        result = b''
        with DataFileWriter(output, DatumWriter(), schema) as writer:
            for record in records:
                writer.append(record)
            writer.flush()
            result = writer.writer.getvalue()
        return result

    def serialize(self, record: dict, schema_json: str) -> bytes:
        """
        Serialize a single record.
        :param record:
        :param schema_json:
        :return: binary string value.
        """
        return self._serialize_list([record], schema_json)
