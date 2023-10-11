import logging
from typing import Callable, Union

import pandas as pd
import polars as pl
import regex as re
from sqlalchemy import text
from sqlalchemy.engine import Connection

import sql_field_report.constants.datatypes as dtypes
import sql_field_report.constants.field_report_schema as schema

logger = logging.getLogger(__name__)


def most_common(lst):
    """
    Return the most common element in a list
    """
    return max(set(lst), key=lst.count)


def estimate_crm_datatype(value, choice_flag):
    """
    Estimate the datatype of a particular value

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

    if re.search(dc_number_regex, value):
        # test if it is a DealCloud number field
        currency_indicators = ["m", "M", "bn", "B", "£", "$", "€", "GBP", "USD", "EUR"]
        if bool(
            [indicator for indicator in currency_indicators if (indicator in value)]
        ):
            return dtypes.CURRENCY
        elif "%" in value:
            return dtypes.PERCENTAGE
        elif "x" in value.lower():
            return dtypes.MULTIPLIER
        else:
            return dtypes.NUMBER
    else:
        if value == "" or value == " ":
            return dtypes.EMPTY
        elif re.search(email_regex, value):
            return dtypes.EMAIL
        elif re.search(date_regex, value):
            return dtypes.DATETIME
        elif len(value) > dtypes.MULTI_LINE_THRESHOLD:
            return dtypes.MULTI_LINE
        elif choice_flag:
            return dtypes.CHOICE_REFERENCE
        elif len(value) < dtypes.MULTI_LINE_THRESHOLD:
            return dtypes.SINGLE_LINE
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
    # determine counts for abridged analysis
    data = pl.from_pandas(pd.read_sql(text(f"SELECT * FROM {table}"), conn))

    return data


# noinspection PyArgumentList
def analyze_data(
    table: Union[str, tuple],
    get_data: Callable[[str], pl.DataFrame],
    cnx: str = None,
    **kwargs,
) -> list[tuple]:
    """Analyze data

    Args:
        table (str): the object/table name - will be passed to the get_data function
        get_data (Callable[[str], pl.DataFrame]):  A function that will take in a table name and return a Dataframe
        cnx (object): ConnectorX Connection object

    Returns:
        tuple: A tuple describing the shape of the data
    """
    logger.info(f"Analysing {table}...")
    res = []
    if cnx:
        data = get_data(table, cnx, **kwargs)
    else:
        data = get_data(table, **kwargs)
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
                datatype = dtypes.EMPTY
            else:
                types = list(
                    [
                        estimate_crm_datatype(v, choice_flag)
                        for v in i.select(pl.col(column)).head().to_series().to_list()
                    ]
                )
                datatype = most_common(types)
            top_five = "; ".join(
                filter(
                    lambda x: x is not None,
                    list(
                        [
                            str(x)[:50] if x != "" else None
                            for x in i.head(6).to_dict(as_series=False).get(column)
                        ]
                    ),
                )
            )

            choices = ""
            if datatype == dtypes.CHOICE_REFERENCE:
                choices = i.select(pl.col(column)).to_dict(as_series=False).get(column)

            report.append(
                (table, column, length, populated, unique, datatype, top_five, choices)
            )
    else:
        for i in data.columns:
            column = i
            populated = 0
            unique = 0
            datatype = "EMPTY"
            top_five = ""
            choices = ""

            report.append(
                (table, column, length, populated, unique, datatype, top_five, choices)
            )

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
        columns=schema.FIELD_REPORT_SCHEMA,
    )

    return analysis


def analyze_polars_dataframes(
    objects: list, get_data: Callable[[str], pl.DataFrame], cnx: str = None, **kwargs
) -> pd.DataFrame:
    """
    Analyze Files

    Params:
    list db_tables - list of database tables
    conn - sql server connection

    Returns:
    pd.DataFrame: analysis - a summary of all files, fields and their row counts
    """

    data_shapes = tuple(analyze_data(l, get_data, cnx, **kwargs) for l in objects)

    # flatten tuple
    data_shapes = tuple((element for t in data_shapes for element in t))

    analysis = pd.DataFrame(
        data=data_shapes,
        columns=schema.FIELD_REPORT_SCHEMA,
    )

    return analysis
