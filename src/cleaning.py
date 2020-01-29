import ast
import logging
import datetime as dt

import pandas as pd
from pandas.core.dtypes.inference import is_list_like


TODAY = dt.datetime.today()
_DEBUG = False


def _get_list(item, errors="ignore"):
    """
    Return a list from the item passed.
    If the item passed is a string, put it in a list.
    If the item is list like, then return it as a list.
    If the item is None, then the return depends on the errors state
        If errors = 'raise' then raise an error if the list is empty
        If errors = 'ignore' then return None
        If errors = 'coerce' then return an empty list if possible
    :param item: either a single item or a list-like
    :param return_empty: if True then return an empty list rather than None
    :return:
    """
    ret_val = None
    if item is None:
        if errors == "coerce":
            ret_val = []
        elif errors == "raise":
            raise ValueError(
                f"Value of item was {item} expected either a single value or list-like"
            )
    elif is_list_like(item):
        ret_val = list(item)
    else:
        ret_val = [item]
    return ret_val


def _get_column_list(df, columns=None):
    """
    Get a list of the columns in the dataframe df.  If columns is None, then return all.
    If columns has a value then it should be either a string (col-name) or a list
    :param df: the dataframe for which to get columns
    :param columns: either the name of a column or a list of columns
    :return: a list of columns that match the string or the list passed
    """
    if columns == "all":
        return list(df.columns)
    else:
        cols = _get_list(columns)
        return (
            list(df.columns)
            if cols is None
            else list(set(df.columns).intersection(cols))
        )


def dump_df_desc(description=""):
    """
    This is a decorator to provide the shape of the dataframe prior to running the function and also after
    :return:
    """

    def wrap(func):
        fname = func.__name__

        def echo_func(*args, **kwargs):
            logging.info(description)
            dataframe = args[0]
            logging.debug(f"Shape prior to {fname}:{dataframe.shape}")
            cols = set(dataframe.columns)
            ret_val = func(*args, **kwargs)
            logging.debug(f"Shape after running {fname}:{ret_val.shape}")
            cols_after = set(ret_val.columns)
            col_diff = cols.difference(cols_after)
            if len(col_diff) == 0:
                logging.debug("No columns dropped.")
            else:
                logging.debug(f"Columns dropped: {col_diff}")
            return ret_val

        return echo_func

    return wrap


def replace_string_in_col_name(df, columns=None, find_val=" ", replace_val=""):
    """
    Find and replace a substring in a column name and replace it with another name
    For instance, spaces to be removed or replaced with _ or eliminate post-merge suffixes
    :param df:
    :param columns:
    :param find_val:
    :param replace_val:
    :return:
    """
    rename_cols = _get_column_list(df, columns)
    rename_cols = df.columns if rename_cols is None else rename_cols
    return df.rename(columns={c: c.replace(find_val, replace_val) for c in rename_cols})


def replace_strings(df, replacements):
    """
    Replace some of the strings that I've seen with counterparts that make sense.  I'll try to keep this list up to date
    :param df:
    :param replacements: A dictionary with values to replace
    :return:
    """
    return df.replace(replacements)


@dump_df_desc()
def remove_columns(df, columns, ignore_errors=True):
    """
    This function is here so that we can capture shape before/after for debugging
    :param df:
    :param columns: either a single column name or a list of columns
    :param ignore_errors:
    :return:
    """
    drop_columns = _get_column_list(df, columns)
    error_handling = "ignore" if ignore_errors else "raise"
    return df.drop(columns=drop_columns, errors=error_handling)


@dump_df_desc(description="Convert columns to True/False from 1/0")
def convert_to_bool(df, columns):
    """
    Convert the list of columns, provided in the
    :param df:
    :param columns:
    :return:
    """
    d_map = {0: False, 1: True}
    for col in _get_column_list(df, columns):
        if df[col].dtype != "bool":
            logging.debug(f"Converting column to boolean - {col}")
            df[col] = df[col].map(d_map, na_action="ignore")
    return df


@dump_df_desc(description="Convert boolean columns to 1/0")
def convert_from_bool(df, columns):
    """
    Convert the list of columns, provided in the
    :param df:
    :param columns:
    :return:
    """
    d_map = {False: 0, True: 1}
    for col in _get_column_list(df, columns):
        if df[col].dtype == "bool":
            logging.debug(f"Converting column from boolean - {col}")
            df[col] = df[col].map(d_map, na_action="ignore")
    return df


@dump_df_desc(description="Convert columns to date columns")
def convert_to_date(df, columns):
    """
    Convert the list of columns to date only columns
    :param df: The dataframe with the columns to convert
    :param columns: {'all', list, or string}.
    :return:
    """
    for col in _get_column_list(df, columns):
        logging.debug(f"Converting column to datetime - {col}")
        df[col] = pd.to_datetime(df[col], errors="ignore")
    return df


@dump_df_desc(description="Dropping rows with not enough relevant data")
def remove_na_rows(df, how="any", threshold=None, subset=None):
    """
    Drop out any rows that don't have at least `threshold` number of values in the columns specified
    This is just a wrapper for the built-in dropna so that I can capture the shape before and after
    :param df: the dataframe to work on
    :param how:
    :parameter threshold:
    :parameter subset:
    :rtype pd.DataFrame:
    :return:
    """
    return df.dropna(axis=0, how=how, thresh=threshold, subset=subset)


# TODO: Write a test for this function
@dump_df_desc("Removing duplicate columns based on index")
def remove_duplicates(df):
    """
    Remove duplicated rows
    :param df:
    :return:
    """
    return df.groupby(df.index).first()


# TODO: Write unit test for this function
def merge_and_fill_gaps(df, left_column, right_column):
    logging.info("filling holes")
    pre_merge_zeros = count_empty_rows(df, column=left_column)
    logging.debug(f"Rows with 0 or NaN prior to merge {pre_merge_zeros}")
    df[left_column] = df.apply(lambda x: max(x[left_column], x[right_column]), axis=1)
    post_merge_zeros = count_empty_rows(df, column=left_column)
    logging.debug(f"Rows with 0 or NaN post to merge {post_merge_zeros}")
    return df


def count_empty_rows(df, column):
    return (df[column].isna().sum()) + (df[column] == 0).sum()


def get_dict_from_string(s):
    try:
        d = eval(s)
    except:
        d = {}
    return d


def split_merged(df):
    """
    Given a merged dataset return the two parts (those that matched and those that didn't)
    :param df:
    :return:
    """
    df_missing = df[df["_merge"] != "both"]
    df = df[df["_merge"] == "both"]
    df.drop(columns=["_merge"], errors="ignore", inplace=True)
    df_missing.drop(columns=["_merge"], errors="ignore", inplace=True)
    return df, df_missing


# TODO: Need a test for this method
def force_data_types(df, map, columns="all", errors="ignore"):
    """

    :param df:
    :param map:
    :param errors:
    :return:
    """
    cols = _get_column_list(df, columns)
    for c in cols:
        col_type = map.get(c, None)
        if col_type is None:
            logging.debug(f"No conversion available for column {c}.")
            continue
        if col_type == "date":
            df[c] = pd.to_datetime(df[c]).date()
        else:
            df[c] = df[c].astype(col_type)


def text_to_dict(df, columns="all"):
    cols = _get_column_list(df, columns)
    for column in cols:
        df[column] = df[column].apply(
            lambda x: {} if pd.isna(x) else ast.literal_eval(x)
        )
    return df
