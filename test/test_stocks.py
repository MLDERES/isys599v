from unittest import TestCase
from src.stocks import getCompanyInfo,get_sp500
import time
import pynance as pn


def TestGetCompanyInfo(TestCase):
    def test_getCompanyInfo(self):
        d = getCompanyInfo("MSFT")
        print(d)
        d2 = getCompanyInfo(['MSFT','NVDA'])
        print(d2)
        print (type(d2))

    def test_getSP500(self):
        start = time.time()
        get_sp500()
        end = time.time()
        print(f'Function took {end-start:0.2f}')

    def test_getPrices(self):
        geopt, geexp = pn.opt.get('ge').info()
        print(geopt)
        print(geexp)
