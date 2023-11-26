from abc import ABC, abstractmethod
from typing import Dict, Any


class AbstractQueryBuilder(ABC):
    @abstractmethod
    def query_create(self, edge_class_name: str, vertex_class_name: str, data: Dict[str, Any]) -> str:
        """Create a query to create a new entity."""
        pass

    @abstractmethod
    def query_retrieve(self, *args, **kwargs) -> str:
        """Create a query to retrieve an entity by its ID."""
        pass

    @abstractmethod
    def query_update(self, rid: str, data: Dict[str, Any]) -> str:
        """Create a query to update an existing entity by its ID."""
        pass

    @abstractmethod
    def query_delete(self, rid: str) -> str:
        """Create a query to delete an entity by its ID."""
        pass
