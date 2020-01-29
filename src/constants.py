import datetime as dt
from pathlib import Path

ROOT = Path(__file__).parent
DATA_PATH = ROOT/'data'
MODEL_PATH = ROOT/'models'
DS_RAW = DATA_PATH/'raw'
DS_PROCESSED = DATA_PATH/'processed'
DS_INTERIM = DATA_PATH/'interim'
DS_EXTERNAL = DATA_PATH/'external'
FALSE_VALUES = ['No', 'no', 'n', 'N','F','False', 'FALSE']
TRUE_VALUES = ['Yes', 'yes', 'y', 'Y','T','True','TRUE']
TODAY = dt.datetime.today()
NOW = dt.datetime.now()
_DEBUG = False
DEFAULT_LOGFILE = 'assignment1'
