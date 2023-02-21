
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from phil_utils.utils import logger, config

# setup the bigquery client
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']
# starting a variable name with _ is python convention to say 
# this is a private module variable and should not be imported outside of this module
# instead use the `get_client()` method
_client: bigquery.Client = None


def get_client() -> bigquery.Client:
    """
    returns a bigquery client to the current project

    Returns:
        bigquery.Client: bigquery client
    """
    # check to see if the client has not been initialized
    global _client
    if _client is None:
        # initialize the client
        _client = bigquery.Client(project=PROJECT_NAME)
        logger.info(f"successfully created bigquery client. project={PROJECT_NAME}")
    return _client


# Define table schemas
# ---------------------------------

# tornadoes table
TORNADOES_TABLE_SCHEMA = [
            # indexes are written if only named in the schema
            bigquery.SchemaField('date', 'datetime', mode='NULLABLE'),
            bigquery.SchemaField('tornado_number', 'int64', mode='REQUIRED'),
            bigquery.SchemaField('year', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('month', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('day', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('timezone', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('state', 'string', mode='NULLABLE'),
            bigquery.SchemaField('magnitude', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('injuries', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('fatalities', 'int64', mode='NULLABLE'),
            bigquery.SchemaField('property_loss', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('crop_loss', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('starting_lat', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('starting_lon', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('end_lat', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('end_lon', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('length', 'float64', mode='NULLABLE'),
            bigquery.SchemaField('width', 'int64', mode='NULLABLE')
]

# global dict to hold all tables schemas
TABLE_SCHEMAS = {
    'tornadoes': TORNADOES_TABLE_SCHEMA,
}


def create_table(table_name: str) -> None:
    """
    Creates bigquery table. Table name must be one of the defined
    table schemas: airports, airlines, routes, aircraft

    Args:
        table_name (str): one of the following table names: airports, airlines, routes, aircraft
    """
    # raise an error if table name if not in one of our schemas
    assert table_name in TABLE_SCHEMAS, f"Table schema not found for table name: {table_name}"

    # full table id: project.dataset.table
    client = get_client()
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"
    # drop existing table if it exists
    try:
        table = client.get_table(table_id)      # table exists if this line doesn't raise exception
        client.delete_table(table)
        logger.info(f"dropped existed bigquery table: {table_id}")
        # wait a couple seconds before creating the table again
        time.sleep(2.0)
    except NotFound:
        # it's OK! table didn't exist
        pass
    # create the table
    schema = TABLE_SCHEMAS[table_name]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=False)
    logger.info(f"created bigquery table: {table_id}")
