import json
import sys
import requests
from avro import schema

# Common accept header sent
ACCEPT_HDR="application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"

class AvroSchemaRegistryClient:
    """
    A client that talks to a Schema Registry over HTTP

    See http://confluent.io/docs/current/schema-registry/docs/intro.html
    """
    def __init__(self, url):
        """Construct a client by passing in the base URL of the schema registry server"""

        self.url = url.rstrip('/')
        # Local cache: id => avro_schema
        self.id_to_schema = {}

    def _send_get_request(self, url):
        response = requests.get(
            url,
            headers={"Accept": ACCEPT_HDR}
        )

        if response.status_code == 404:
            return None

        return response.json()

    def get_by_schema_id(self, schema_id):
        """Retrieve a parsed avro schema by id or None if not found"""
        if schema_id in self.id_to_schema:
            return self.id_to_schema[schema_id]
        # fetch from the registry
        url = '/'.join([self.url,'schemas','ids',str(schema_id)])

        response = self._send_get_request(url)
        if not response:
            return None

        return schema.parse(response.get("schema"))
