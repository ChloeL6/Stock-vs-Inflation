import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from phil_utils.utils import logger, config, DATA_DIR

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

def tornados_transformations():
  # load tornadoes file
  tornadoes_df = pd.read_csv(os.path.join(DATA_DIR, config['tornadoes']))

  # Drop columns
  drop_cols = [
    'stf',
    'stn',
    'ns',
    'sn',
    'sg',
    'f1',
    'f2',
    'f3',
    'f4',
    'fc',
  ]

  tornadoes_df = tornadoes_df.drop(columns=drop_cols)

  logger.info('Dropped columns.')

  