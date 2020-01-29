import os
from pathlib import Path

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
