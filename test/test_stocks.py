from unittest import TestCase
from src.stocks import get_sp500, StockChart
import time


def TestGetCompanyInfo(TestCase):
    def test_getSP500(self):
        start = time.time()
        get_sp500()
        end = time.time()
        print(f'Function took {end - start:0.2f}')


class TestStockChart:

    def test_load_from_ticker(self):
        ticker = StockChart.LoadFromTicker('MSFT')
        assert ticker is not None
