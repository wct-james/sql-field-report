import logging
import traceback
from typing import Callable

import pandas as pd
import polars as pl
import regex as re
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


def get_series(table: str, data: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    """Accepts a dataframe and returns a list of series

    Args:
        data (pd.DataFrame): Dataframe to be broken down

    Returns:
        list[pd.Series]: List of series for each dataframe column
    """
    return list([(table, c, data[c]) for c in data.columns])


def get_sql_polars(arg: tuple[str, Connection]) -> pl.DataFrame:
    """
    Take in a file path and return an overview of the shape of that file

    Params:
    str: table - the database table to query

    Returns:
    Tuple: file_shape - a tuple of tuples containing the file name, field name, row count for each field
    """
    table, conn = arg
    data = pl.from_pandas(pd.read_sql(text(f"SELECT * FROM {table}"), conn))

    return data


def analyze_data(table: str, get_data: Callable[[str], pl.DataFrame]) -> tuple:
    """Analyze data

    Args:
        table (str): the object/table name - will be passed to the get_data function
        get_data (Callable[[str], pl.DataFrame]):  A function that will take in a table name and return a Dataframe

    Returns:
        tuple: A tuple describing the shape of the data
    """
    res = []
    data = get_data(table)
    if isinstance(table, tuple):
        table = table[0]
    length = data.shape[0]
    report = []
    if length != 0:
        for i in data.columns:
            res.append(
                data.select(pl.col(i).value_counts(sort=True)).select(
                    [
                        pl.col(i).struct.field(i),
                        pl.col(i).struct.field("counts"),
                    ]
                )
            )

        for i in res:
            column = i.columns[0]
            if i.dtypes[0] == pl.Utf8:
                e = i.filter(
                    (pl.col(column) == "") | (pl.col(column).is_null())
                ).select(pl.col("counts"))
                if e.shape[0] > 0:
                    empty = e.rows(named=True)[0].get("counts")
                else:
                    empty = 0
            elif i.dtypes[0] in pl.INTEGER_DTYPES or i.dtypes[0] in pl.FLOAT_DTYPES:
                e = i.filter(pl.col(column).is_null() | pl.col(column).is_nan()).select(
                    pl.col("counts")
                )
                if e.shape[0] > 0:
                    empty = e.rows(named=True)[0].get("counts")
                else:
                    empty = 0
            else:
                e = i.filter(pl.col(column).is_null()).select(pl.col("counts"))
                if e.shape[0] > 0:
                    empty = e.rows(named=True)[0].get("counts")
                else:
                    empty = 0
            populated = length - empty
            if populated == 0:
                unique = 0
            else:
                unique = i.shape[0]

            choice_ratio = float(unique) / float(length)
            choice_flag = get_choice_flag(unique, choice_ratio, length)

            if populated == 0:
                datatype = "EMPTY"
            else:
                types = list(
                    [
                        estimate_dealcloud_datatype(v, choice_flag)
                        for v in i.select(pl.col(column)).head().to_series().to_list()
                    ]
                )
                datatype = most_common(types)

            logger.info(
                "Analyzed: {}".format(
                    str((table, column, length, populated, unique, datatype))
                )
            )

            report.append((table, column, length, populated, unique, datatype))
    else:
        for i in data.columns:
            column = i
            populated = 0
            unique = 0
            datatype = "EMPTY"

            report.append((table, column, length, populated, unique, datatype))

    return report


def analyze_sql_tables(objects: list, conn: Connection) -> pd.DataFrame:
    """
    Analyze SQL Tables

    Params:
    list db_tables - list of database tables
    conn - sql server connection

    Returns:
    pd.DataFrame: analysis - a summary of all files, fields and their row counts
    """

    # data_shapes = tuple(analyze_sql_table(l, conn) for l in objects)
    data_shapes = tuple(analyze_data((l, conn), get_sql_polars) for l in objects)

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


def analyze_polars_dataframes(
    objects: list, get_data: Callable[[str], pl.DataFrame]
) -> pd.DataFrame:
    """
    Analyze Files

    Params:
    list db_tables - list of database tables
    conn - sql server connection

    Returns:
    pd.DataFrame: analysis - a summary of all files, fields and their row counts
    """

    data_shapes = tuple(analyze_data(l, get_data) for l in objects)

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
