import numpy as np
import pandas as pd

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