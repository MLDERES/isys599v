import datetime as dt
import logging
import pandas as pd
from IPython.core.display import display
from joblib import dump, load
from multiprocessing_logging import install_mp_handler
from src.constants import *

TODAY = dt.datetime.today()
NOW = dt.datetime.now()
_DEBUG = False

def start_logging(debug_to_console=False, log_filename=DEFAULT_LOGFILE):
    logging.basicConfig(level=logging.DEBUG,
                        datefmt='%m-%d %H:%M',
                        filename=(ROOT / log_filename).with_suffix('.log'),
                        filemode='a', format='%(asctime)s: %(levelname)-8s %(message)s')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setLevel(logging.INFO if not debug_to_console else logging.DEBUG)
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    install_mp_handler()


def write_excel(data, filename, data_version=False, folder=DS_INTERIM, with_ts=True, **kwargs):
    """
    Write multiple data items to a single Excel file.  Where the data is a dictionary of
    datasources and dataframes
    :param data: dictionary of sheet names and dataframes
    :param filename: the name of the excel file to save
    :param folder: folder to store the excel file
    :param with_ts: if true, add a timestamp to the filename
    :param kwargs: other arguments to be passed to the pandas to_excel function
    :return: the filename of the excel file that was written
    """
    logger = logging.getLogger(__name__)
    logger.info(f"writing {len(data)} to excel... {folder}")
    fn = make_ts_filename(DATA_PATH / folder, filename, suffix='.xlsx', with_ts=with_ts)

    if 'float_format' not in kwargs.keys():
        kwargs['float_format'] = '%.3f'
    if type(data_version) is bool:
        data_version = f'_{TODAY.month:02d}{TODAY.day:02d}' if data_version else ''

    with pd.ExcelWriter(fn) as writer:
        for datasource, df in data.items():
            if type(df) is not pd.DataFrame:
                continue
            df.to_excel(writer, sheet_name=f'{datasource}{data_version}', **kwargs)
    logger.info(f"finished writing df to file... {filename}")
    return filename


def make_ts_filename(dir_name, src_name, suffix, with_ts=True):
    """
    Get a path with the filename specified by src_name with or without a timestamp, in the appropriate directory
    :param dir_name:
    :param src_name:
    :param suffix:
    :param with_ts:
    :return:
    """
    NOW = dt.datetime.now()
    filename_suffix = f'{TODAY.month:02d}{TODAY.day:02d}_{NOW.hour:02d}{NOW.minute:02}{NOW.second:02d}' \
        if with_ts else "latest"
    fn = f'{src_name}_{filename_suffix}'
    suffix = suffix if '.' in suffix else f'.{suffix}'
    filename = (dir_name / fn).with_suffix(suffix)
    return filename


# TODO: Work out Enums for datasource and data state
#  So an example would be a file that is cleaned and combines two sources would have an output name
#   of source1_source2_clean or src1_src2_features
def write_data(df, datasource_name, folder=DS_INTERIM, with_ts=True, **kwargs):
    """
    Export the dataset to a file
    :param df: the dataset to write
    :param datasource_name: the basefilename to write
    :param folder: the data subpath (one of 'interim', 'processed', 'external'
    :param with_ts: if True, then append the year, month, day and hour to the filename to be written
                    else append the suffix 'latest' to the basename
    :param idx: the name of the index or the column number
    :return: the name of the file written
    """
    NOW = dt.datetime.now()
    logger = logging.getLogger(__name__)
    logger.info(f"writing df to file... {datasource_name} {folder}")
    fn = make_ts_filename(DATA_PATH / folder, src_name=datasource_name, suffix='.csv')

    if 'float_format' not in kwargs.keys():
        kwargs['float_format'] = '%.3f'
    df.to_csv(fn, **kwargs)
    logger.info(f"finished writing df to file... {fn}")
    return fn


def write_model(mdl, model_type, with_ts=True, **kwargs):
    """
    Pickle and write a model to the model directory
    :param mdl: the object to save
    :param model_type: this will be the filename e.g. 'lbgm' -> lgbm_033120.mdl
    :param with_ts: if true append the datetime to the filename else 'latest'
    :param kwargs:
    :return: the filename of the file that was saved
    """
    NOW = dt.datetime.now()
    logger = logging.getLogger(__name__)
    logger.info(f"exporting model to file... {model_type} ")
    path_to_write = MODEL_PATH
    filename_suffix = f'{TODAY.month:02d}{TODAY.day:02d}_{NOW.hour:02d}{NOW.minute:02}{NOW.second:02d}' \
        if with_ts else "latest"
    fn = f'{model_type}_{filename_suffix}'
    filename = (path_to_write / fn).with_suffix('.mdl')
    dump(mdl,filename)
    logger.info(f"finished writing model to file... {filename}")
    return filename

def read_latest_model(model_type):
    NOW = dt.datetime.now()
    read_path = MODEL_PATH
    logger = logging.getLogger(__name__)
    logger.info(f"reading model from file... {model_type} ")
    fname = get_latest_file(file_path=read_path, filename_like=model_type, file_ext='.mdl')
    logging.debug(f"read from {fname}")
    return load(read_path/fname)

def read_latest(datasource_name, folder=DS_INTERIM, **kwargs):
    """
    Get the most recent version of the cleaned dataset
    :param datasource_name: name of the file to get the data from
    :param folder: the subpath to the data, likely interim or processed
    :return:
    """
    assert folder in [DS_EXTERNAL,DS_INTERIM, DS_PROCESSED, DS_RAW], \
        f"Invalid folder to read '{folder}'"
    read_path = DATA_PATH / folder
    fname = get_latest_data_filename(datasource_name, folder)
    logging.info(f"read from {fname}")
    return pd.read_csv(read_path / fname, index_col=0, infer_datetime_format=True, true_values=TRUE_VALUES,
                       false_values=FALSE_VALUES, **kwargs)


def read_latest_from_worksheet(filename, datasource_name='all', folder=DS_INTERIM, **kwargs):
    """
    Get the most recent version of the cleaned dataset
    :param datasource_name: name of the worksheet to get the data from
            or 'all'.  If 'all' then returns a dictionary of datasets keyed from datasource_name
    :param folder: the subpath to the data
    :param filename: the stubname of the file to open.
    :return: Either a dataset that is made up of the data from a single sheet or a dictionary of datasets keyed on the
      sheet name
    """
    assert folder in [DS_EXTERNAL, DS_INTERIM, DS_PROCESSED, DS_RAW], f"Invalid folder to read '{folder}'"
    logger = logging.getLogger(__name__)
    read_path = DATA_PATH / folder
    fname = get_latest_data_filename(filename, folder, file_ext='.xlsx')
    logger.info(f"read {datasource_name} from {fname}")
    if datasource_name == 'all':
        ret_val = pd.read_excel(read_path / fname, sheet_name=None, index_col=0, infer_datetime_format=True, **kwargs)
    else:
        # TODO: Get a single sheet with the closest datasource name or a list of sheets
        ret_val = pd.read_excel(read_path / fname, sheet_name=datasource_name, index_col=0,
                                infer_datetime_format=True, **kwargs)
    return ret_val


def get_latest_data_filename(datasource_name, folder, file_ext='.csv'):
    """
    Determine the filename of the latest version of this file source
    :param folder: the folder to look for the file represented by the `datasource_name`
    :param datasource_name: the basename of the datafile.  For instance if the datasource_name is foo then the filename
          representing the latest modified file with a name like 'foo*' will be returned
    :return:
    """
    return get_latest_file(DATA_PATH / folder, datasource_name, file_ext)

def get_latest_file(file_path, filename_like, file_ext):
    """
    Find absolute path to the file with the latest timestamp given the datasource name and file extension in the path
    :param file_path: where to look for the file
    :param filename_like: the basename of the datafile.  For instance if the datasource_name is foo then the filename
          representing the latest modified file with a name like 'foo*' will be returned
    :param file_ext: the filename extension
    :return: the absolute path to the file
    """
    file_ext = file_ext if '.' in file_ext else f'.{file_ext}'
    all_files = [f for f in file_path.glob(f'{filename_like}*{file_ext}',)]
    assert len(all_files) > 0, f'Unable to find any files like {file_path / filename_like}{file_ext}'
    fname = max(all_files, key=lambda x: x.stat().st_mtime).name
    return fname


def get_latest_dataset_label(datasource_name, folder):
    fn = get_latest_data_filename(datasource_name, folder)
    return fn.rsplit('_', 1)[0]

def get_file_version_from_name(fn):
    return fn.split('_')[1]

def display_all(df):
    with pd.option_context("display.max_rows", 1000, "display.max_columns", 1000):
        display(df)

def missing_percentage(df):
    '''
    Get the percentage of missing values in a dataset
    :param df:
    :return:
    '''
    total = df.isnull().sum().sort_values(ascending = False)
    percent = (df.isnull().sum()/df.isnull().count()*100).sort_values(ascending = False)
    missing_data  = pd.concat([total, percent], axis=1, keys=['Total', 'Percent'])
    return missing_data.head(20)
