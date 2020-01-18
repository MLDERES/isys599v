from functools import lru_cache

import pandas as pd
from src.cleaning import _get_list
from src.utils import *
from dotenv import load_dotenv
from iexfinance.stocks import Stock, get_historical_data
from typing import List, Union, Dict
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
