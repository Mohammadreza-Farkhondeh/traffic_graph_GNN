from typing import List, Dict, Any

import pandas as pd
from pyorient import OrientDB

from src.graph_database.base_client import BaseGraphDBClient
from src.graph_database.orientdb.managers import VertexManager, EdgeManager, SchemaManager


class OrientDBClient(BaseGraphDBClient):
    """
    Concrete Graph database client implementation for OrientDB.

    Methods:
    - connect(): Connects to the OrientDB database.
    - close_connection(): Closes the connection to the OrientDB database.
    - define_schema(properties: Dict[str, Any]): Defines the schema for the OrientDB database.
    - ingest_data(data: Dict[str, Any]): Ingests data into the OrientDB database.
    - get_edge_manager(): Returns the EdgeManager for OrientDB.
    - get_vertex_manager(): Returns the VertexManager for OrientDB.
    - get_schema_manager(): Returns the SchemaManager for OrientDB.
    """
    def __init__(self, host: str, port: int, username: str, password: str, database: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client: OrientDB = None
        self.vertex_manager: VertexManager = None
        self.edge_manager: EdgeManager = None
        self.schema_manager: SchemaManager = None
        self._connect()

    def _connect(self):
        self.client = OrientDB(self.host, self.port)
        _ = self.client.connect(self.username, self.password)
        self.client.db_open(self.database, self.username, self.password)
        self.schema_manager = SchemaManager(self.client)
        self.edge_manager = EdgeManager(self.client)
        self.vertex_manager = VertexManager(self.client)

    def define_schema(self, schema: List[Dict[str, Any]]) -> None:
        for cls in schema:
            try:
                self.schema_manager.create(cls['name'], cls['properties'])
            except KeyError as e:
                raise "name and properties should be in each dict in list"

    async def ingest_dataframe(self, df: pd.DataFrame, classname: str, is_edge: bool = True, vertex_class_name: str = 'V') -> None:
        """
        Ingests data into the OrientDB database.

        :param classname: classname in database
        :param df: The data to be ingested.
        :param is_edge: Boolean indicating whether the data represents edges. Defaults to False.
        :param vertex_class_name: if is_edge = true, then need vertex class for edges. default is V

        """
        for index, record in df.iterrows():
            try:
                if not self.schema_manager.class_exists(classname):
                    await self.schema_manager.create(classname, {}, extends='E' if is_edge else 'V')
            except KeyError as e:
                raise "key, didnt exist, @class should be in data"
            try:
                print('_______')
                print(record.to_dict())
                print('_______')
                if is_edge:
                    await self.edge_manager.create(edge_class_name=classname,
                                                   vertex_class_name=vertex_class_name,
                                                   data=record.to_dict())
                else:
                    await self.vertex_manager.create(class_name=classname,
                                                     data=record.to_dict())
            except KeyError as e:
                raise "Key didnt exist, from and to should be in is_edge=True"

    def close_connection(self):
        self.client.db_close()

    def get_edge_manager(self) -> EdgeManager:
        """
        Get the manager for edge operations.

        :return: Edge manager.
        """
        return self.edge_manager

    def get_vertex_manager(self) -> VertexManager:
        """
        Get the manager for vertex operations.

        :return: Vertex manager.
        """
        return self.vertex_manager

    def get_schema_manager(self) -> SchemaManager:
        """
        Get the manager for schema operations.

        :return: schema manager.
        """
        return self.schema_manager
