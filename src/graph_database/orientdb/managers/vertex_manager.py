from typing import Any, Dict

from pyorient.orient import OrientDB

from .abstract_manager import AbstractManager
from ..query_builders import VertexQueryBuilder


class VertexManager(AbstractManager):
    """
    concrete manager for `orientdb Vertices`
    implements abstract manager
    methods will execute command and queries on orientdb
    """

    def __init__(self, client: OrientDB):
        self.client = client
        self._query_builder = VertexQueryBuilder()

    async def create(self, class_name: str, data: Dict[str, Any]) -> Dict:
        d = {f"@{class_name}": data}
        result = self.client.record_create(-1, d)
        return result

    async def update(self, rid: str, data: Dict[str, Any]) -> Dict:
        query = self._query_builder.query_update(rid, data)
        result = self.client.command(query)
        return result[0].__dict__

    async def delete(self, rid: str) -> bool:
        cluster, instance_id = map(int, rid.split("#")[1].split(":"))
        result = self.client.record_delete(cluster, instance_id)
        return result

    async def retrieve(self, class_name: str, vertex_filter: str = "1=1") -> Dict:
        query = self._query_builder.query_retrieve(class_name, vertex_filter)
        result = self.client.query(query)
        return result
