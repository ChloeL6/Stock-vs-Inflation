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
  tornadoes_df = pd.read_csv(os.path.join(DATA_DIR, config['tornadoes']), header=0)

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

  # Rename columns
  renamed = {
    'date_time':'date',
    'om':'tornado_number',
    'yr':'year',
    'mo':'month',
    'dy':'day',
    'tz':'timezone',
    'st':'state',
    'mag':'magnitude',
    'inj':'injuries',
    'fat':'fatalities',
    'loss':'property_loss',
    'closs':'crop_loss',
    'slat':'starting_lat',
    'slon':'starting_lon',
    'elat':'end_lat',
    'elon':'end_lon',
    'len':'length',
    'wid':'width'
  }

  tornadoes_df = tornadoes_df.rename(columns=renamed)

  logger.info('Renamed columns.')

  # Change data type
  tornadoes_df['state'] = tornadoes_df['state'].astype('string')

  logger.info('Explicitly set data types.')
