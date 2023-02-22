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
  tornadoes_df = pd.read_csv()