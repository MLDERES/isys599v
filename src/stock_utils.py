import numpy as np
import pandas as pd

# the column name suffixes used to ensure consistency in referencing the data
MOVING_AVG = 'moving_avg'
_colnames = {MOVING_AVG:'moving_avg'}

def make_column_name(period, col):
    """
    The purpose of this function is to ensure that create columns (from the Calculate*
    functions) are consistent and don't have to be known to the caller

    Example:
    30_DMA = GetColumnName(30,MOVING_AVERAGE)
    :param period:
    :param col:
    :return:
    """
    column_suffix = _colnames.get(col, "")
    assert column_suffix != "", f'No such column: "{col}"'
    return f'{period}_{column_suffix}'

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