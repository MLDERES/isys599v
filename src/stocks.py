import string
import os, sys
from enum import Enum
from typing import List, Union
from dotenv import load_dotenv
from src.utils import *
from src.constants import *
import math
from dateutil.relativedelta import  relativedelta
import numpy as np
import pandas as pd
from alpha_vantage.timeseries import TimeSeries

ANN_TRADE_DAYS = 252
load_dotenv()
EX_API_KEY = os.getenv('IEX_TOKEN')
ALPHA_API = os.getenv('ALPHA_VANTAGE_TOKEN')

# Provides the ability to avoid having to create this everywhere
_today = pd.Timestamp(TODAY)

class SortOrder(Enum):
    ASC = 'asc'
    DESC = 'desc'

    def previous_day(self):
        return -1 if self == SortOrder.ASC else 1

def get_sp500():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df


def get_sp500_tickers():
    df = get_sp500()
    assert isinstance(df, pd.DataFrame)
    return df['Symbols']



def getLikelyPrice(starting_price, historical_returns, period):
    """
    Using a monte-carlo like simulation to calculate the price of an asset using historical returns
    NOTE: the historical returns need to be in the same units as period, so if the historical returns
    are calculated daily then it is assumed tha the period is in number of days
    :param historical_returns: Series of returns in the same frequency as <period>
    :param period: the period over which to calculate the return
    """
    daily_prices = np.zeros(period)
    daily_prices[0] = starting_price
    for x in range(period-1):
        sample_yield = historical_returns.sample(n=1)
        daily_prices[x+1] = daily_prices[x]*(sample_yield +1)
    m = daily_prices.mean()
    if (np.isnan(m)):
        print(f"Got nan:\n{daily_prices}")

    return daily_prices.mean()

def _getHistoricalTicker(ticker, full=False):

    # Using AlphaVantage https://alpha-vantage.readthedocs.io/en/latest/
    # As of now, this just gets the last 100 days if I need more I'll have to do specify
    #  outputsize = 'full' which would bring back everything.  In that case, I'd likely try to load it from
    #  the CSV file instead and then merge together
    ts = TimeSeries(ALPHA_API, output_format='pandas')
    meta = None
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
            # If we have old data, then we'll go back to the source and get new data

            if relativedelta(pd.to_datetime(historical.index[-1]), TODAY).days > 0:
                last_100, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='compact')
                last_100.rename(columns={'1. open': OPEN_PRICE, '2. high': DAY_HIGH,
                                           '3. low': DAY_LOW, '4. close': DAY_CLOSE,
                                           '5. adjusted close': ADJ_CLOSE, '6. volume': DAY_VOLUME,
                                           '7. dividend amount': DIVIDEND_AMT, '8. split coefficient': SPLIT_COEFFICIENT}
                                  , inplace=True)
                ret_val = historical.append(last_100)
                ret_val.drop_duplicates(inplace=True)
                write_data(ret_val,ticker,folder=DS_EXTERNAL)
            else:
                ret_val = historical
    else:
        last_100, meta = ts.get_daily_adjusted(symbol=ticker,outputsize='compact')
        ret_val = last_100
    return ret_val, meta


class ColumnNames(Enum):
    MOVING_AVG = 'moving_avg'
    ANNUAL_VOL = 'annualized_volatility'
    TOTAL_PRICE_RETURN = 'total price return'
    TOTAL_PRICE_RETURN_FWD = 'total price return (fwd)'
    PCT_DAY_CHANGE = 'pct daily change'

    def make_column_name(self, period, period_type='d'):
        """
        The purpose of this function is to ensure that create columns (from the Calculate*
        functions) are consistent and don't have to be known to the caller

        Example:
        30_DMA = GetColumnName(30,MOVING_AVERAGE)
        :param period: the number of days in the column to get
        :param period_type: one of (day, week, month or year) 'd', 'w', 'm' or 'y'
        :return:
        """
        return f'{period}{period_type} {self.value}'


class StockChart:

    # Should look a lot like a DataGrid indexed on Date
    # Could also have a set of information associated with the company
    # Should have a variety of functions we can apply to the data such as
    #    - Getting the daily return rate
    #    - Determining the 30 day annualized volatility
    #    - Determining the annualized return
    ASC = -1
    DESC = 1

    def __init__(self, ticker):
        self.data = None
        self.ticker = ticker
        self._sort_order = None

    @property
    def sort_order(self):
        if self._sort_order is None:
            self._sort_order = SortOrder.ASC if df.index[1] < df.index[-1] else SortOrder.DESC
        return self._sort_order

    def sort(self, direction=SortOrder.DESC):
        self.data.sort_index(ascending=(direction == SortOrder.ASC),inplace=True)

    def _column_exists(self,col_name):
        return self.data.columns.contains(col_name)

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
    def LoadFromFile(cls, ticker, errors='raise', folder=DS_PROCESSED, sort_order=SortOrder.ASC, **kwargs):
        """
        :param folder:
        :param kwargs:
        :return:
        """
        chart = cls(ticker)
        chart.data = read_latest(ticker, folder=folder, errors=errors)
        chart.sort(sort_order)
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
            col_name = ColumnNames.MOVING_AVG.make_column_name(days)
            self.data[col_name] = new_col
        return new_col

    def CalculateTotalReturnPrice(self, forward_adjusted=False):
        """
        Add a column for daily price return accounting for dividends and splits

        **Total Price Return**
        The prices in this series are adjusted on dividend ex-date by reducing the price prior to the dividend payment
        by applying a dividend adjustment factor calculated as (1 - Value of Dividend/ Previous Day's Close Price).

        **Total Price Return (Forward Adjusting)**
        The prices in this series are adjusted on dividend ex-date by increasing the price AFTER
        the dividend payment by applying a dividend adjustment factor
        calculated as (1 + Value of Dividend/ Previous Day's Close Price).

        reference https://ycharts.com/glossary/terms/total_return_price

        :param forward_adjusted: If true, then the forward adjusted approach will be used
        :return: None.  The chart will be updated with a column that has the daily price data
        """
        if not self._column_exists(ColumnNames.TOTAL_PRICE_RETURN):
            prev_day = self.sort_order.previous_day()
            if forward_adjusted:
                self.data[ColumnNames.TOTAL_PRICE_RETURN] = \
                    df[DAY_CLOSE]*df[SPLIT_COEFFICIENT]*(1+df[DIVIDEND_AMT]/df[DAY_CLOSE].shift(prev_day))
            else:
                self.data[ColumnNames.TOTAL_PRICE_RETURN] = \
                    df[DAY_CLOSE] * df[SPLIT_COEFFICIENT] * (1 - df[DIVIDEND_AMT] / df[DAY_CLOSE].shift(prev_day))
        return None

    def Calculate30DayAnnualizedVolatility(self):
        """
        https://ycharts.com/glossary/terms/rolling_vol_30
        :return:
        """
        #The standard deviation of of a rolling 30 day percent_change * sqrt (252)
        if self._column_exists(ColumnNames.ANNUAL_VOL):
            return
        self.CalculatePercentDailyChange()
        df = self.data
        # Square root of trading days
        srt = math.sqrt(ANN_TRADE_DAYS)
        df[ColumnNames.ANNUAL_VOL] =  df[ColumnNames.TOTAL_PRICE_RETURN].rolling(30).std(min_periods=30) * srt

    def CalculatePercentDailyChange(self):
        # If the Total Price Return hasn't been calculated, do that first
        if self._column_exists(ColumnNames.PCT_DAY_CHANGE):
            return
        self.CalculateTotalReturnPrice()
        prev_day = self.sort_order.previous_day()
        df = self.data
        df[ColumnNames.PCT_DAY_CHANGE] = df[ColumnNames.TOTAL_PRICE_RETURN].pct_change(prev_day)

    def LastDays(self,days, trading_days=True):
        """
        Create a dataset that includes encompasses a certain number of days in the past
        :param days: the number of days to go back
        :param trading_days: if True consider only business days otherwise all days
        :return: a pandas dataframe with only the number of days specified going back from today
        """
        offset = pd.offsets.BDay(days) if trading_days else pd.Timedelta(day=-days)
        return self.data[_today-offset:]

    def LastWeeks(self, weeks):
        """
        Create a dataset that includes encompasses a certain number of days in the past
        :param weeks: the number of weeks to go back from today
        :return: a pandas dataframe with only the number of weeks specified, starting from today and going backward
        """
        return self.data[_today-pd.DateOffset(weeks=weeks):]


if __name__ == "__main__":
    #print(ColumnNames.TOTAL_PRICE_RETURN.make_column_name(30,'d'))
    msft = StockChart.LoadFromTicker('MSFT')
    msft = StockChart.LoadFromFile('MSFT', folder=DS_EXTERNAL)
    print(msft.data.columns)
    print(msft._column_exists(ColumnNames.PCT_DAY_CHANGE))
    print(msft._column_exists(DAY_CLOSE))

    # msft.SaveChart()
    # print(f'{msft.data}')
    #msft = StockChart.LoadFromTicker('MSFT')

