import requests
from avro import schema

# Common accept header sent
ACCEPT_HDR = "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"


class AvroSchemaRegistryClient:
    """
    A client that talks to a Schema Registry over HTTP

    See http://confluent.io/docs/current/schema-registry/docs/intro.html
    """

    def __init__(self, url):
        """Construct a client by passing in the base URL of the schema registry server"""

        self.url = url.rstrip('/')
        # Local cache: id => "subject", "version", "id", "schema"
        self.id_to_data = {}
        # Local cache of parsed schemas.
        self.id_to_schema = {}
        # Local cache: name to latest ID.
        self.subject_to_latest = {}

    @staticmethod
    def _send_get_request(url):
        response = requests.get(
            url,
            headers={"Accept": ACCEPT_HDR}
        )

        response.raise_for_status()

        return response.json()

    def _retrieve_and_cache_all_schemas(self):
        # If we already have the cache, we don't have to do anything.
        if self.id_to_data:
            return
        # Retrieves ALL schemas, with id, version, subject and schema (json string).
        response = self._send_get_request(self.url + "/schemas")
        for item in response:
            schema_id = item['id']
            version = item['version']
            subject = item['subject']
            # Set main cache.
            self.id_to_data[schema_id] = item
            # And make sure newest version is in here.
            if subject not in self.subject_to_latest or \
                    self.subject_to_latest[subject]['version'] < version:
                self.subject_to_latest[subject] = item

    def get_schema_id_by_subject(self, subject):
        """ Get latest schema ID by subject. """
        self._retrieve_and_cache_all_schemas()
        if subject not in self.subject_to_latest:
            raise Exception("Schema not found in registry?")

        return self.subject_to_latest[subject]['id']

    def get_by_schema_id(self, schema_id):
        """Retrieve a parsed avro schema by id or None if not found"""
        self._retrieve_and_cache_all_schemas()
        if schema_id not in self.id_to_data:
            raise Exception("Schema ID not found in registry?")

        if schema_id not in self.id_to_schema:
            if schema_id not in self.id_to_data:
                return None

            schema_json = self.id_to_data[schema_id]['schema']
            self.id_to_schema[schema_id] = schema.parse(schema_json)

        return self.id_to_schema[schema_id]

    def get_subject_by_schema_id(self, schema_id):
        """ Get the subject by schema ID. """
        self._retrieve_and_cache_all_schemas()
        if schema_id not in self.id_to_data:
            raise Exception("Schema ID not found in registry?")
        return self.id_to_data[schema_id]['subject']
