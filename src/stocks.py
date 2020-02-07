import string
from typing import List, Union
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from iexfinance.stocks import Stock
from src.utils import *
from alpha_vantage.timeseries import TimeSeries
StringList = List[str]

# Get the IEX_API key from the environment variable or .env file
load_dotenv()
EX_API_KEY = os.getenv('IEX_TOKEN')
ALPHA_API = os.getenv('ALPHA_VANTAGE_TOKEN')



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
    sym = get_list(symbols, errors='raise')
    stock_batch = Stock(sym, token=EX_API_KEY, output_format='pandas')
    return stock_batch


def _getHistoricalTicker(ticker, full=False):

    # Using AlphaVantage https://alpha-vantage.readthedocs.io/en/latest/
    # As of now, this just gets the last 100 days if I need more I'll have to do specify
    #  outputsize = 'full' which would bring back everything.  In that case, I'd likely try to load it from
    #  the CSV file instead and then merge together
    ts = TimeSeries(ALPHA_API, output_format='pandas')
    if full:
        historical = read_latest(ticker, folder=DS_EXTERNAL, errors='ignore')
        if historical is None:
            historical, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='full')
            write_data(historical,ticker,folder=DS_EXTERNAL)
            ret_val=historical
        else:
            last_100, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='compact')
            ret_val = historical.append(last_100)
            ret_val.drop_duplicates(inplace=True)
            write_data(ret_val,ticker,folder=DS_EXTERNAL)
    else:
        last_100, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='compact')
        ret_val = last_100
    return ret_val, meta





class StockChart:

    # Should look alot like a DataGrid indexed on Date
    # Could also have a set of information associated with the company
    # Should have a variety of functions we can apply to the data such as
    #    - Getting the daily return rate
    #    - Determining the 30 day annualized volatility
    #    - Determining the annualized return

    def __init__(self):
        self._data = None

    @classmethod
    def LoadFromTicker(cls, ticker: string):
        """
        Given a particular ticker symbol, download historical data starting from today going backward
        :param ticker:
        :param startDate:
        :param endDate:
        :return: A pandas dataframe with an index col of date
        """
        chart = cls()
        chart._data, meta = _getHistoricalTicker(ticker, full=True)
        return chart

    def CalculateMovingAvg(self, timeframe, colName):
        pass

    def Calculate30DayAnnualizedVolatility(self):
        pass


if __name__ == "__main__":
    #df_msft, meta = _getHistoricalTicker('MSFT', full=True)
    msft = StockChart.LoadFromTicker('MSFT')

