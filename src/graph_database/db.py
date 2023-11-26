from src.config.config_loader import ConfigLoader
from src.graph_database.orientdb_client import OrientDBClient
from src.graph_database.base_client import BaseGraphDBClient


class DB:

    def __init__(self):
        self.config = ConfigLoader.load_config()
        self.client = self._init_client()

    def _init_client(self) -> BaseGraphDBClient:
        graph_type = self.config.get('database', {}).get('type', '').lower()

        if graph_type == 'orientdb':
            return OrientDBClient(
                host=self.config.get('database', {}).get('host', ''),
                port=self.config.get('database', {}).get('port', 2424),
                username=self.config.get('database', {}).get('username', ''),
                password=self.config.get('database', {}).get('password', ''),
                database=self.config.get('database', {}).get('database', '')
            )
        else:
            raise ValueError(f"Unsupported graph type: {graph_type}")
