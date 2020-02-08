import string
from typing import List, Union
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from iexfinance.stocks import Stock
from src.utils import *
from src.constants import *
from src.stock_utils import *

from alpha_vantage.timeseries import TimeSeries
StringList = List[str]

# Get the IEX_API key from the environment variable or .env file
load_dotenv()
EX_API_KEY = os.getenv('IEX_TOKEN')
ALPHA_API = os.getenv('ALPHA_VANTAGE_TOKEN')

# Provides the ability to avoid having to create this everywhere
_today = pd.Timestamp(TODAY)



def get_sp500():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df


def get_sp500_tickers():
    df = get_sp500()
    assert isinstance(df, pd.DataFrame)
    return df['Symbols']

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
            historical.rename(columns={'1. open':OPEN_PRICE, '2. high':DAY_HIGH,
                                       '3. low':DAY_LOW, '4. close':DAY_CLOSE,
                                       '5. adjusted close':ADJ_CLOSE, '6. volume':DAY_VOLUME,
                                       '7. dividend amount':DIVIDEND_AMT, '8. split coefficient':SPLIT_COEFFICIENT}
                              ,inplace=True)
            write_data(historical,ticker,folder=DS_EXTERNAL)
            ret_val=historical
        else:
            last_100, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='compact')
            historical.rename(columns={'1. open': OPEN_PRICE, '2. high': DAY_HIGH,
                                       '3. low': DAY_LOW, '4. close': DAY_CLOSE,
                                       '5. adjusted close': ADJ_CLOSE, '6. volume': DAY_VOLUME,
                                       '7. dividend amount': DIVIDEND_AMT, '8. split coefficient': SPLIT_COEFFICIENT}
                              , inplace=True)
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

    def __init__(self, ticker):
        self.data = None
        self.ticker = ticker

    @classmethod
    def LoadFromTicker(cls, ticker: string):
        """
        Given a particular ticker symbol, download historical data starting from today going backward
        :param ticker:
        :param startDate:
        :param endDate:
        :return: A pandas dataframe with an index col of date
        """
        chart = cls(ticker)
        chart.data, meta = _getHistoricalTicker(ticker, full=True)
        return chart

    @classmethod
    def LoadFromFile(cls, ticker, errors='raise', folder=DS_PROCESSED, **kwargs):
        """
        :param folder:
        :param kwargs:
        :return:
        """
        chart = cls(ticker)
        chart.data = read_latest(ticker, folder=folder, errors=errors)
        return chart

    def Save(self, with_timestamp=True, folder=DS_PROCESSED, **kwargs):
        """
        Write the data we have out to disk
        :return: None
        """
        write_data(self.data, self.ticker, with_ts=with_timestamp, folder=folder, **kwargs)

    def CalculateMovingAvg(self, days, update=True):
        """
        Calculate the moving average for a specific number of days giving the number of days
        :param days:
        :param update: If True then update the dataframe with the data, else just return the value
        :return: a series with calculated moving average
        """
        new_col = self.data[DAY_CLOSE].rolling(days).mean()
        if update:
            col_name = make_column_name(days,MOVING_AVG)
            self.data[col_name] = new_col
        return new_col

    def Calculate30DayAnnualizedVolatility(self):
        pass

    def LastDays(self,days, trading_days=True):
        offset = pd.offsets.BDay(days) if trading_days else pd.Timedelta(day=-days)
        return self.data[_today-offset:]

    def LastWeeks(self, weeks):
        return self.data[_today-pd.DateOffset(weeks=weeks):]



if __name__ == "__main__":
    msft = StockChart.LoadFromFile('MSFT', folder=DS_EXTERNAL)
    msft.SaveChart()
    print(f'{msft.data}')
    #msft = StockChart.LoadFromTicker('MSFT')

