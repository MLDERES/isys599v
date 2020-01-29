from functools import lru_cache

import pandas as pd
from src.utils import *
from dotenv import load_dotenv
from iexfinance.stocks import Stock, get_historical_data
from typing import List, Union, Dict
import numpy as np
StringList = List[str]

# Get the IEX_API key from the environment variable or .env file
load_dotenv()
EX_API_KEY = os.getenv('IEX_TOKEN')
#EX_API_KEY = os.getenv('IEX_TEST_TOKEN')




def get_sp500():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df


def get_sp500_tickers():
    df = get_sp500()
    assert isinstance(df, pd.DataFrame)
    return df['Symbols']


def getCompanyInfo(symbols: Union[str, StringList]) -> pd.DataFrame:
    """
    Query IEX for information about a list of stocks
    :param symbols: a single symbol as a string or a list of strings representing symbols
    :return: a Pandas dataframe
    """
    stock_batch = _get_stocks(symbols)
    company_info = stock_batch.get_company()
    return company_info.T


def getPrices(symbol):
    sym = _get_stocks(symbol)
    return sym.get_close().T

def getHistorical(symbol, start, end):
    sym = _get_stocks(symbol)
    return sym.get_historical_prices(start=start, end=end).T


def _get_stocks(symbols):
    sym = _get_list(symbols, errors='raise')
    stock_batch = Stock(sym, token=EX_API_KEY, output_format='pandas')
    return stock_batch

def _get_list(item, errors='ignore'):
    """
    Return a list from the item passed.
    If the item passed is a string, put it in a list.
    If the item is list like, then return it as a list.
    If the item is None, then the return depends on the errors state
        If errors = 'raise' then raise an error if the list is empty
        If errors = 'ignore' then return None
        If errors = 'coerce' then return an empty list if possible
    :param item: either a single item or a list-like
    :param return_empty: if True then return an empty list rather than None
    :return:
    """
    retVal = None
    if item is None:
        if errors=='coerce':
            retVal = []
        elif errors=='raise':
            raise ValueError(f'Value of item was {item} expected either a single value or list-like')
    elif is_list_like(item):
        retVal = list(item)
    else:
        retVal = [item]
    return retVal

def _get_column_list(df, columns=None):
    '''
    Get a list of the columns in the dataframe df.  If columns is None, then return all.
    If columns has a value then it should be either a string (col-name) or a list
    :param df:
    :param columns:
    :return:
    '''
    cols = _get_list(columns)
    return list(df.columns) if cols is None else list(set(df.columns).intersection(cols))