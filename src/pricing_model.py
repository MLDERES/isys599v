import math
from scipy.stats import norm
from dateutil.parser import *
from datetime import *

def BlackSholes(currentPrice, strikePrice, volatility, rate, expiration = '12/31/2020'):
    term = (parse(expiration).date() - date.today()).days
    return _BlackSholes(currentPrice,strikePrice,volatility,rate, term, termUnits='days')

def _BlackSholes(currentPrice, strikePrice, volatility, rate, term, termUnits='days'):
    # this is the log of the current price / strike price term
    logStockStrike = math.log(currentPrice/strikePrice)
    # represents the sigma-squared divided by 2 term
    sigmaDiv2 = (volatility**2)/2
    # Convert days to part of years
    T = term/365 if termUnits == 'days' else term
    ert = math.exp(-rate*T)

    d1 = (logStockStrike + (rate+sigmaDiv2)*T) / (volatility*math.sqrt(T))
    d2 = d1 - volatility*math.sqrt(T)
    Nd1 = norm.cdf(d1)
    Nd2 = norm.cdf(d2)
    callPrice = currentPrice*Nd1 - strikePrice*ert*Nd2

    Nnegd1 = norm.cdf(-d1)
    Nnegd2 = norm.cdf(-d2)
    putPrice = strikePrice*ert*Nnegd2 - currentPrice*Nnegd1

    return (callPrice, putPrice)

if __name__ == "__main__":
    import numpy as np
    import pandas as pd

    df = pd.DataFrame({'vol':np.arange(15,25,0.1)})
    df['call'] = df['vol'].apply(lambda x: BlackSholes(166.36, 175, x, 0.017,'12/31/2020')[0])
    df

