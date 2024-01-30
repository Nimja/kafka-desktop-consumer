import json
from hashlib import md5
import requests
from avro import schema
from ..exception import AvroSchemaException

# Common accept header sent
ACCEPT_HDR = "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


TIMEOUT = 10


class AvroSchemaRegistryClient:
    """
    A client that talks to a Schema Registry over HTTP

    See http://confluent.io/docs/current/schema-registry/docs/intro.html
    """

    def __init__(self, url):
        """Construct a client by passing in the base URL of the schema registry server"""

        self.url = url.rstrip('/')
        # Local cache: id => "schema"
        self.id_to_json = {}
        # Local cache of parsed schemas.
        self.id_to_schema = {}
        # Hashed schemas, restructured schemas to go from JSON string to hash.
        self.md5_to_id = {}

    @staticmethod
    def _send_get_request(url):
        response = requests.get(
            url,
            headers={"Accept": ACCEPT_HDR},
            timeout=TIMEOUT,
        )

        response.raise_for_status()

        return response.json()

    @staticmethod
    def _send_post_request(url, data):
        response = requests.post(
            url,
            json=data,
            headers={
                "Accept": ACCEPT_HDR,
                "Content-Type": CONTENT_TYPE,
            },
            timeout=TIMEOUT,
        )

        response.raise_for_status()

        return response.json()

    def _schema_json_to_md5(self, json_string):  # pylint: disable=no-self-use
        """
        Go from json string to consistent MD5.

        Multiple subjects using the same schema, use the same ID as well in schema registry.
        """
        reformatted = json.dumps(json.loads(json_string), sort_keys=True)  # Sorted order, for MD5.
        md5_hash = md5(reformatted.encode('utf-8')).hexdigest()
        return f"{md5_hash}"

    def _registry_post_store_schema(self, subject, json_string):
        """
        Store schema if it does not exist and get ID.
        """
        data = {
            "schema": json.dumps(json.loads(json_string), sort_keys=False)
        }
        full_url = f"{self.url}/subjects/{subject}/versions"
        response = self._send_post_request(full_url, data)
        return response["id"]

    def _registry_post_find_by_schema(self, subject, json_string):
        """
        Find schema, if it exists, and return ID.
        """
        data = {
            "schema": json.dumps(json.loads(json_string), sort_keys=False)
        }
        full_url = f"{self.url}/subjects/{subject}"
        response = self._send_post_request(full_url, data)
        return response["id"]

    def _store_schema_by_id(self, schema_id, schema_json):
        self.md5_to_id[self._schema_json_to_md5(schema_json)] = schema_id
        self.id_to_json[schema_id] = schema_json

    def _registry_get_schema_by_id(self, schema_id):
        schema_id = int(schema_id)
        response = self._send_get_request(f"{self.url}/schemas/ids/{schema_id}")
        return response["schema"]

    def get_or_create_schema_id_by_subject_and_json(self, subject, json_string, auto_create=True):
        """ Get schema ID by subject and schema json, if present or create if not. """
        lookup_hash = self._schema_json_to_md5(json_string)
        # Not found yet.
        if lookup_hash not in self.md5_to_id:
            if auto_create: # This will get-or-create by schema.
                schema_id = self._registry_post_store_schema(subject, json_string)
                self._store_schema_by_id(schema_id, json_string)
            else: # This will only get, by schema.
                schema_id = self._registry_post_find_by_schema(subject, json_string)
                self._store_schema_by_id(schema_id, json_string)

        # Finally retrieve it.
        return self.md5_to_id.get(lookup_hash)

    def get_by_schema_id(self, schema_id):
        """Retrieve a parsed avro schema by id or None if not found"""
        if not schema_id:
            raise AvroSchemaException("No schema ID given!")

        if schema_id not in self.id_to_schema:
            schema_json = self.get_json_by_schema_id(schema_id)
            self.id_to_schema[schema_id] = schema.parse(schema_json)

        return self.id_to_schema[schema_id]

    def get_json_by_schema_id(self, schema_id):
        """
        Get the unparsed json schema.

        Will get it from schema registry if needed.
        """
        if not schema_id:
            raise AvroSchemaException("No schema ID given!")

        if schema_id not in self.id_to_json:
            schema_json = self._registry_get_schema_by_id(schema_id)
            self._store_schema_by_id(schema_id, schema_json)

        if schema_id not in self.id_to_json:
            raise AvroSchemaException("Schema ID not found in registry?")

        return self.id_to_json[schema_id]

    def validate_schema_against_latest(self, subject, json_string):
        """
        Ask an avro schema to be validated against the latest version of the existing schema.
        """
        data = {
            "schema": json.dumps(json.loads(json_string), sort_keys=False)
        }
        full_url = f"{self.url}/compatibility/subjects/{subject}/versions/latest?verbose=true"
        response = self._send_post_request(full_url, data)
        is_compatible = response.get("is_compatible", False)
        messages = response.get("messages", [])
        return bool(is_compatible), messages
