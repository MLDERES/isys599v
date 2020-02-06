from src import stocks
import pandas as pd
from datetime import *
from dateutil.relativedelta import *
from src.constants import TODAY

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
    def LoadFromTicker(cls,ticker,startDate=None, endDate=TODAY):
        chart = cls()
        if (startDate is None):
            startDate = endDate + relativedelta(years=-1)
        chart._data = stocks.getHistorical(ticker,startDate,endDate)
        return chart

    def LoadHistoricalData(self, start, end):
        pass

    def CalculateMovingAvg(self,timeframe, colName):
        pass

    def Calculate30DayAnnualizedVolatility(self):
        pass

if __name__ == '__main__':
    df = StockChart.LoadFromTicker('MSFT')._data
    print(df)
