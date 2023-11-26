import os
from src.data_ingestion.seed_xls_to_dataframe_converter import XlsToDatasetConverter
from src.data_ingestion.record_featured_dataframe_converter import RecordFeaturedToOrientDBConverter
from src.graph_database.db import DB
from src.graph_database.orientdb_client import OrientDBClient
import anyio


async def main():
    seed_data_directory = os.getcwd().replace('src', 'seed/')
    # Step 1: Convert Excel files to DataFrame
    xls_converter = XlsToDatasetConverter(seed_data_directory)
    dataset_df = xls_converter.convert()
    # print(dataset_df.head())

    # Step 2: Convert to OrientDB-compatible DataFrame
    orientdb_converter = RecordFeaturedToOrientDBConverter()
    orientdb_df = orientdb_converter.convert_to_desired_format(dataset_df, 'Road', frmt='orientdb')
    # print(orientdb_df.head())

    # Step 3: Ingest data into OrientDB
    database = DB()
    orientdb_client: OrientDBClient = database.client
    # print(orientdb_client.client.query('select * From Road')[0].__dict__)

    # Define schema if not already defined
    schema = [
        {'name': 'Road', 'properties': {'source': 'STRING', 'destination': 'STRING', 'data': 'EMBEDDEDLIST'}}
    ]

    orientdb_client.define_schema(schema)

    # Ingest data
    await orientdb_client.ingest_dataframe(orientdb_df, 'Road', is_edge=True)

    # Close connection
    orientdb_client.close_connection()


if __name__ == "__main__":
    anyio.run(main)
