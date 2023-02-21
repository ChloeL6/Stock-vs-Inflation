import logging
import sys
import os
import yaml

from airflow.models import Variable

# -------------------------------------------
# Set up logging
# -------------------------------------------

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=logging.INFO,
                    stream=sys.stderr)

# define logger
logger: logging.Logger = logging.getLogger()
logger.setLevel(level=logging.INFO)


def get_this_dir(filepath: str = __file__) -> str:
    """helper function to return this (python) file directory"""
    return os.path.dirname(os.path.abspath(filepath))


# -------------------------------------------
# Load config file
# -------------------------------------------

# default config file path when running locally (not within airflow docker container)
# get the path from airflow variables OR set it to default local path
_default_config_path = os.path.join(get_this_dir(), '../phil_config.yaml')
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    logger.info(f"loaded configurations file: {CONF_PATH}")


# Set data dir path
# ---------------------------------------------
# default config file path when running locally (not within airflow docker container)
# get the path from airflow variables OR set it to default local path
_default_data_dir_path = os.path.join(get_this_dir(), '../data')
DATA_DIR = Variable.get('data_dir', default_var=_default_data_dir_path)