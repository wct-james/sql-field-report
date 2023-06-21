import logging
from typing import Callable

import time

import pandas as pd
from multiprocessing import Pool
import traceback
import regex as re
import sys
from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def most_common(lst):
    """
    Return the most common element in a list
    """
    return max(set(lst), key=lst.count)


def estimate_dealcloud_datatype(value, choice_flag):
    """
    Estimate the DealCloud datatype of a particular value

    Params:
    value
    bool: choice_flag

    Returns:
    str: datatype
    """
    value = str(value)
    dc_number_regex = r"^[0-9.,%xXmMbnB$£€GBPUSDEUR]+$"
    email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    date_regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|\d{2,4}-\d{2}-\d{2,4}|\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}|\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}|\d{2,4}\/\d{2}\/\d{2,4})"
    url_regex = r"[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"

    if choice_flag:
        return "Choice/Reference"
    elif re.search(dc_number_regex, value):
        # test if it is a DealCloud number field
        currency_indicators = ["m", "M", "bn", "B", "£", "$", "€", "GBP", "USD", "EUR"]
        if bool(
            [indicator for indicator in currency_indicators if (indicator in value)]
        ):
            return "Currency"
        elif "%" in value:
            return "Percentage"
        elif "x" in value.lower():
            return "Multiplier"
        else:
            return "Number"
    else:
        if value == "" or value == " ":
            return "EMPTY"
        elif re.search(email_regex, value):
            return "E-mail"
        elif re.search(date_regex, value):
            return "Date and Time"
        elif len(value) < 125:
            return "Single Line"
        elif len(value) > 125:
            return "Multi Line"
        else:
            return None


def get_choice_flag(distinct_count, choice_ratio, count) -> bool:
    """
    Determine if a field should be a choice or not

    Params:
    int: distinct_count - the unique values
    float: choice_ratio - the ratio of unique values:values
    int: count - the count of values

    Returns:
    bool: True if choice else false
    """
    if (
        (float(distinct_count) < 10 or float(choice_ratio) < 0.05)
        and (not float(distinct_count) == 0)
        and (not float(count) == float(distinct_count))
    ):
        return True
    else:
        return False


def review_column(column_data: tuple[str, str, pd.Series]):
    """
    Review a column of data in a data set to return field report metadata

    Params:
    str: file - The source file/table
    tuple[str, pd.Series]: column - a tuple containing the column name and the column in a pd.Series

    Return:
    tuple: analysis - the column analysis
    """

    file, column, data = column_data

    if data.shape[0] != 0:
        length = len(data)
        values = data.dropna().to_list()
        populated = len(values)
        unique = len(set(values))

        if length > 0:
            choice_ratio = float(unique) / float(length)
        else:
            choice_ratio = length

        choice_flag = get_choice_flag(unique, choice_ratio, length)

        values = data.to_list()

        types = list([estimate_dealcloud_datatype(v, choice_flag) for v in set(values)])

        datatype = most_common(types)

        analysis = (file, column, length, populated, unique, datatype)

        logger.info("Analyzed: {}".format(str(analysis)))

    else:
        analysis = (file, column, 0, 0, 0, "EMPTY")

    return analysis


def get_series(table: str, data: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    """Accepts a dataframe and returns a list of series

    Args:
        data (pd.DataFrame): Dataframe to be broken down

    Returns:
        list[pd.Series]: List of series for each dataframe column
    """
    return list([(table, c, data[c]) for c in data.columns])


def analyze_sql_table(table: str, conn: Connection):
    """
    Take in a file path and return an overview of the shape of that file

    Params:
    str: table - the database table to query

    Returns:
    Tuple: file_shape - a tuple of tuples containing the file name, field name, row count for each field
    """
    try:
        data = pd.read_sql(text(f"SELECT * FROM {table}"), conn)

        data = get_series(table, data)

        with Pool() as p:
            file_shape = p.map(review_column, data)
    except:
        data = pd.DataFrame.from_records(
            data=[["ERROR", "ERROR"]], columns=["ERROR", "ERROR2"]
        )
        data = get_series(data)
        file_shape = tuple((review_column(h) for h in data))

    return file_shape


def analyze_dataframe(table: str, get_data: Callable[[str], pd.DataFrame]) -> tuple:
    """Analyze a dataframe

    Args:
        table (str): the object/table name - will be passed to the get_data function
        get_data (Callable[[str], pd.DataFrame]): A function that will take in a table name and return a Dataframe

    Returns:
        tuple: A tuple describing the shape of the data
    """
    try:
        data = get_data(table)
        data = get_series(table, data)

        with Pool() as p:
            file_shape = p.map(review_column, data)
    except:
        traceback.print_exc()
        data = pd.DataFrame.from_records(
            data=[["ERROR", "ERROR"]], columns=["ERROR", "ERROR2"]
        )
        data = get_series(data)
        file_shape = tuple((review_column(h) for h in data))
    return file_shape


def analyze_sql_tables(objects: list, conn: Connection) -> pd.DataFrame:
    """
    Analyze SQL Tables

    Params:
    list db_tables - list of database tables
    conn - sql server connection

    Returns:
    pd.DataFrame: analysis - a summary of all files, fields and their row counts
    """

    data_shapes = tuple(analyze_sql_table(l, conn) for l in objects)

    # flatten tuple
    data_shapes = tuple((element for t in data_shapes for element in t))

    analysis = pd.DataFrame(
        data=data_shapes,
        columns=[
            "Table/File",
            "Field",
            "Count",
            "Populated",
            "Unique",
            "Datatype",
            # "Choices",
        ],
    )

    return analysis


def analyze_dataframes(
    objects: list, get_data: Callable[[str], pd.DataFrame]
) -> pd.DataFrame:
    """
    Analyze Files

    Params:
    list db_tables - list of database tables
    conn - sql server connection

    Returns:
    pd.DataFrame: analysis - a summary of all files, fields and their row counts
    """

    data_shapes = tuple(analyze_dataframe(l, get_data) for l in objects)

    # flatten tuple
    data_shapes = tuple((element for t in data_shapes for element in t))

    analysis = pd.DataFrame(
        data=data_shapes,
        columns=[
            "Table/File",
            "Field",
            "Count",
            "Populated",
            "Unique",
            "Datatype",
            # "Choices",
        ],
    )

    return analysis
