from abc import ABC, abstractmethod
from typing import List, Dict

import pandas as pd


class BaseGraphDBClient(ABC):
    """
    Abstract base class for graph database clients.

    Attributes:
    - host (str): The host address of the graph database.
    - port (int): The port number for connecting to the graph database.
    - username (str): The username for authentication.
    - password (str): The password for authentication.
    - database_name (str): The name of the graph database.

    :Methods:
    - connect(): Connects to the graph database.
    - define_schema(properties: List[str]): Defines the schema for the graph.
    - ingest_data(data: List[Dict]): Ingests data into the graph.
    - close_connection(): Closes the connection to the graph database.
    """

    @abstractmethod
    def _connect(self):
        """
        Connect to the graph database.
        """
        pass

    @abstractmethod
    def define_schema(self, properties: List[str]):
        """
        Define the schema for the graph.

        Parameters:
        - properties (List[str]): The list of properties to be included in the schema.
        """
        pass

    @abstractmethod
    def ingest_dataframe(self, data: pd.DataFrame, classname: str, is_edge: bool = True):
        """
        Ingest data into the graph.

        Parameters:
        - data (List[Dict]): The data to be ingested into the graph.
        """
        pass

    @abstractmethod
    def close_connection(self):
        """
        Close the connection to the graph database.
        """
        pass

    @abstractmethod
    def get_edge_manager(self):
        """
        Get the manager for edge operations.

        :return: Edge manager.
        """
        pass

    @abstractmethod
    def get_vertex_manager(self):
        """
        Get the manager for vertex operations.

        :return: Vertex manager.
        """
        pass

    @abstractmethod
    def get_schema_manager(self):
        """
        Get the manager for schema operations.

        :return: Schema manager.
        """
        pass
