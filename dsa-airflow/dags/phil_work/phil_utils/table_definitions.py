
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from dsa_utils.utils import logger, config


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
# airport table schema
AIRPORTS_TABLE_SCHEMA = [
    bigquery.SchemaField('iata', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('airport', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('lat', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('lon', 'FLOAT', mode='NULLABLE'),
]

# airlines table
AIRLINES_TABLE_SCHEMA = [
    bigquery.SchemaField("airline_id", 'STRING', mode='REQUIRED'),
    bigquery.SchemaField("name", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("alias", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("iata", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("icao", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("callsign", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("country", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("active", 'STRING', mode='NULLABLE'),
]

# routes table
ROUTES_TABLE_SCHEMA = [
    bigquery.SchemaField('airline', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('src', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('dest', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('codeshare', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('stops', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('equipment', 'STRING', mode='NULLABLE'),
]

# aircraft table
AIRCRAFT_TABLE_SCHEMA = [
    bigquery.SchemaField("n_number", 'STRING', mode='REQUIRED'),
    bigquery.SchemaField("serial_number", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("mfr_mdl_code", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("eng_mfr_mdl", 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField("mfr_year", 'DATETIME', mode='NULLABLE'),
    bigquery.SchemaField("registrant_type", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("registrant_name", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("street", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("street2", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("city", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("state", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("zip_code", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("region", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("country", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("last_action_date", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("issue_date", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("status", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("air_ready_date", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("expiration_date", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("mfr_name", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("mfr_short_name", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("model", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("aircraft_type", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("num_engines", 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField("num_seats", 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField("weight_class", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("speed", 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField("eng_mfr_name", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("eng_model", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("eng_type", 'STRING', mode='NULLABLE'),
    bigquery.SchemaField("horsepower", 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField("thrust", 'FLOAT', mode='NULLABLE'),
]

# global dict to hold all tables schemas
TABLE_SCHEMAS = {
    'airports': AIRLINES_TABLE_SCHEMA,
    'airlines': AIRLINES_TABLE_SCHEMA,
    'routes': ROUTES_TABLE_SCHEMA,
    'aircraft': AIRCRAFT_TABLE_SCHEMA,
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
