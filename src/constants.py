import os
from pathlib import Path
import datetime as dt

ROOT = Path(os.getcwd()).parent
DATA_PATH = ROOT / 'data'
MODEL_PATH = ROOT / 'models'
DEFAULT_LOGFILE = 'logger'

DS_INTERIM = 'interim'
DS_RAW = 'raw'
DS_PROCESSED = 'processed'
DS_EXTERNAL = 'external'

FALSE_VALUES = ['No', 'no', 'n', 'N','F','False', 'FALSE']
TRUE_VALUES = ['Yes', 'yes', 'y', 'Y','T','True','TRUE']

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

### Specific to the stock portfolio
OPEN_PRICE = 'open'
DAY_HIGH = 'high'
DAY_LOW = 'low'
DAY_CLOSE = 'close'
ADJ_CLOSE = 'adjusted close'
DAY_VOLUME = 'volume'
DIVIDEND_AMT = 'dividend amt'
SPLIT_COEFFICIENT = 'split coef'