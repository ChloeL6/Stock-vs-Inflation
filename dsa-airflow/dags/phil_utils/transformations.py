import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# local module imports
from phil_utils.utils import logger, config, DATA_DIR

PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

def tornadoes_transformations():
  # load tornadoes file
  tornadoes_file = os.path.join(DATA_DIR, config['tornadoes'])
  tornadoes_df = pd.read_csv(tornadoes_file, 
    header=0, 
    parse_dates=[['date', 'time']],
    on_bad_lines='warn'
  )
  
  logger.info(f"loaded {len(tornadoes_df.index)} rows from {tornadoes_file}")


  # check schema: contains all expected columns?
  expected_columns = [
      'om',
      'yr',
      'mo',
      'dy',
      'date_time',
      'tz',
      'st',
      'stf',
      'stn',
      'mag',
      'inj',
      'fat',
      'loss',
      'closs',
      'slat',
      'slon',
      'elat',
      'elon',
      'len',
      'wid',
      'ns',
      'sn',
      'f1',
      'f2',
      'f3',
      'f4',
      'fc'
  ]
  for col in expected_columns:
    assert col in list(tornadoes_df.columns), f"Data file missing required column: {col}"

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

  # write to file
  tornadoes_df.to_csv(tornadoes_file, header=True)
