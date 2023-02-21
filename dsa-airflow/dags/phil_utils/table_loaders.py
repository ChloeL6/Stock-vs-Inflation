import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from phil_utils import logger, config, DATA_DIR


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


# global variable to hold data files
DATA_FILES = {
    'tornadoes': os.path.join(DATA_DIR, config['tornadoes'])
}


def load_table(table_name: str):
    """
    Load airports CSV file to BigQuery using 

    Args:
        table_name (str): must be one of the following: airports, airlines, routes, aircraft
    """
    # make sure table_name is one of our data files
    assert table_name in DATA_FILES, f"Unknown table name: {table_name}"
    # get the data file path
    client = get_client()
    data_file = DATA_FILES[table_name]
    # check to see if data file exists
    assert os.path.exists(data_file), f"Missing data file: {data_file}"
    # insert data into bigquery
    table_id = f"{PROJECT_NAME}.{DATASET_NAME}.airports"
    # bigquery job config to load from a csv file
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        max_bad_records=100,
        ignore_unknown_values=True,
    )
    logger.info(f"loading bigquery {table_name} from file: {data_file}")
    with open(data_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    # wait for the job to complete
    job.result()
    # get the number of rows inserted
    table = client.get_table(table_id)
    logger.info(f"inserted {table.num_rows} rows to {table_id}")