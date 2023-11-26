from abc import ABC, abstractmethod
from typing import Dict, Any


class AbstractManager(ABC):
    @abstractmethod
    async def create(self, class_name: str, data: Dict[str, Any]) -> str:
        """Create a new entity."""
        pass

    @abstractmethod
    async def retrieve(self, filters: Dict[str, Any]) -> Any:
        """Retrieve an entity by its ID."""
        pass

    @abstractmethod
    async def update(self, rid: str, data: Dict[str, Any]) -> bool:
        """Update an existing entity by its ID."""
        pass

    @abstractmethod
    async def delete(self, rid: str) -> bool:
        """Delete an entity by its ID."""
        pass

