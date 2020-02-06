class StockChart:

    # Should look alot like a DataGrid indexed on Date
    # Could also have a set of information associated with the company
    # Should have a variety of functions we can apply to the data such as
    #    - Getting the daily return rate
    #    - Determining the 30 day annualized volatility
    #    - Determining the annualized return

    def __init__(self):
        self._data = None
        pass

    def LoadHistoricalData(self, start, end):
        pass

    def CalculateMovingAvg(self,timeframe, colName):
        pass

    def Calculate30DayAnnualizedVolatility(self):
        pass