from typing import Any, Dict

from pyorient.orient import OrientDB

from .abstract_manager import AbstractManager
from ..query_builders import EdgeQueryBuilder


class EdgeManager(AbstractManager):
    """
    concrete manager for `orientdb edges`
    implements abstract manager
    methods will execute command and queries on orientdb
    """

    def __init__(self, client: OrientDB):
        self.client = client
        self._query_builder = EdgeQueryBuilder()

    async def create(self, edge_class_name: str, data: Dict[str, Any], vertex_class_name: str = 'V') -> Dict:
        command = self._query_builder.query_create(edge_class_name, vertex_class_name, data)
        print(command)
        result = self.client.command(command)
        return result[0].__dict__

    async def update(self, rid: str, data: Dict[str, Any]) -> Dict:
        query = self._query_builder.query_update(rid, data)
        i = self.client.command(query)
        result = self.client.record_load(rid)
        return result.__dict__

    async def delete(self, rid: str) -> Any:
        """
        WARNING! WARNING! WARNING!
        used orient.record_delete() and it destroyed the whole database,
        """
        command = self._query_builder.query_delete(rid)
        result = self.client.command(command)
        return result

    async def retrieve(
            self,
            class_name: str,
            out_filter: str = "1=1",
            in_filter: str = "1=1",
            data: Dict[str, Any] = None,
    ) -> Any:
        query = self._query_builder.query_retrieve(
            class_name, out_filter, in_filter, data
        )
        result = self.client.query(query)
        return result