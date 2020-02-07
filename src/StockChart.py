from src.stocks import StockChart

if __name__ == '__main__':
    df = StockChart.LoadFromTicker('MSFT')._data
    print(df)
