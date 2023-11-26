from typing import Any, Dict
from pyorient.orient import OrientDB

from src.graph_database.orientdb.managers.abstract_manager import AbstractManager
from src.graph_database.orientdb.query_builders import SchemaQueryBuilder


class SchemaManager(AbstractManager):
    """
    Concrete manager for handling `orientdb schema`.
    Implements abstract manager, providing methods to execute commands and queries on OrientDB related to the schema.
    """
    def __init__(self, client: OrientDB):
        self.client = client
        self._query_builder = SchemaQueryBuilder()

    async def create(self, class_name: str, properties: Dict[str, Any] = None, extends: str = None) -> Dict:
        """
        Creates a new class in the schema.
        """
        query = self._query_builder.query_create_class(class_name, properties, extends)
        result = self.client.command(query)
        return result[0].__dict__

    async def update(self, class_name: str, properties: Dict[str, Any] = None) -> Dict:
        """
        Updates an existing class in the schema.
        """
        query = self._query_builder.query_update_class(class_name, properties)
        result = self.client.command(query)
        return result[0].__dict__

    async def delete(self, class_name: str) -> Any:
        """
        Deletes a class from the schema.
        """
        query = self._query_builder.query_delete_class(class_name)
        result = self.client.command(query)
        return result

    async def retrieve(self) -> Any:
        """
        Retrieves all classes from the schema.
        """
        query = self._query_builder.query_retrieve_classes()
        result = self.client.query(query)
        return result

    async def class_exists(self, class_name: str) -> bool:
        """
        Check if a class exists in the database.

        :param class_name: The name of the class to check.
        :return: True if the class exists, False otherwise.
        """
        query = f"SELECT count(*) as count FROM (SELECT expand(classes) FROM metadata:s) WHERE name = '{class_name}'"
        result = self.client.command(query)
        return bool(result and result[0].count > 0)