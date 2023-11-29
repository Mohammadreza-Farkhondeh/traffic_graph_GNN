from typing import Any, Dict
from .abstract_query_builder import AbstractQueryBuilder


class SchemaQueryBuilder(AbstractQueryBuilder):
    """
    Concrete query builder for handling `orientdb schema`.
    Implements abstract query builder, providing methods to generate queries for schema operations.
    """
    def query_create(self, class_name: str, extends: str = None) -> str:
        """
        Generates a query for creating a new class in the schema.
        """
        query = f"CREATE CLASS {class_name} IF NOT EXISTS"

        if extends:
            query += f" EXTENDS {extends}"

        return query

    def query_update(self, class_name: str, properties: Dict[str, Any] = None) -> str:
        """
        Generates a query for updating an existing class in the schema.
        """
        # Placeholder code
        pass

    def query_delete(self, class_name: str) -> str:
        """
        Generates a query for deleting a class from the schema.
        """
        query = f"DROP CLASS {class_name}"
        return query

    def query_retrieve(self) -> str:
        """
        Generates a query for retrieving all classes from the schema.
        """
        query = "SELECT expand(classes) FROM metadata:schema"
        return query
