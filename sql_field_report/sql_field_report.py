import logging
from typing import Callable

import coloredlogs
import connectorx as cx
import pandas as pd
import polars as pl
import typer
from sqlalchemy import Connection, text

from .utils.analysis import analyze_polars_dataframes, analyze_sql_tables
from .utils.databases import MSSQLConnectionX, MySQLConnection
from .utils.excel import generate_excel_report


def build_sql_field_report(output_file_name: str, objects: list, conn: Connection):
    """Build SQL Field Report

    Args:
        output_file_name (str): The output file name for the report
        objects (list): A list of tables to be analyzed
        conn: SQLAlchemy connection

    Returns:
        str: SQL Report filepath
    """

    analysis = analyze_sql_tables(objects, conn)

    path = generate_excel_report(analysis, output_file_name)

    if path:
        return path
    else:
        return None


def get_mssql_data(table: str, cnx: str) -> pl.DataFrame:
    """Get MSSQL Data

    Using connectx, query data from a given SQL table

    Args:
        table (str): Database table name
        cnx (str): connectx connection string

    Returns:
        pl.DataFrame: Table data
    """
    try:
        # get number of rows
        counts = (
            cx.read_sql(cnx, f"SELECT COUNT(*) [c] FROM {table}", return_type="polars")
            .select(pl.col("c"))
            .to_series()
            .to_list()[0]
        )
        logger.info(f"Table {table} has: {counts} rows...")

        # get columns with valid datatypes
        t = table.split("].[")[1][:-1]
        cols = (
            cx.read_sql(
                cnx,
                f"SELECT DISTINCT('[' + COLUMN_NAME + ']') [COLUMN_NAME] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{t}' AND DATA_TYPE != 'sql_variant'",
                return_type="polars",
            )
            .select(pl.col("COLUMN_NAME"))
            .to_series()
            .to_list()
        )

        columns = ", ".join(cols)

        if counts > 100000:
            logger.info(f"Table {table} -- abbreviating to top 50000")
            query = f"SELECT TOP(50000) {columns} FROM {table}"
        else:
            query = f"SELECT {columns} FROM {table}"
        data = cx.read_sql(cnx, query, return_type="polars")
        logger.info(f"Table {table} data pulled.")
    except:
        logger.error(f"Table {table} data pull failed.")
        data = pl.from_records(data=[[0]], schema=["ERROR"])
    return data


def build_dataframe_field_report(
    output_file_name: str,
    objects: list,
    get_data: Callable[[str], pl.DataFrame],
    cnx: str = None,
    **kwargs,
):
    """Build DataFrames Field Report

    Args:
        output_file_name (str): The output file name for the report
        objects (list): A list of tables to be analyzed
        get_data (Callable[[str], pd.DataFrame]): A function that will take in a table name and return a Dataframe

    Returns:
        str: SQL Report filepath
    """

    analysis = analyze_polars_dataframes(objects, get_data, cnx, **kwargs)

    path = generate_excel_report(analysis, output_file_name)

    if path:
        return path
    else:
        return None


def build_mssql_field_report(output_file_name: str, objects: list, cnx: str):
    path = build_dataframe_field_report(output_file_name, objects, get_mssql_data, cnx)
    if path:
        return path
    else:
        return None


logger = logging.getLogger(__name__)
coloredlogs.install()
logging.basicConfig(level="INFO")

app = typer.Typer()


@app.command()
def MSSQL_Database_Report(
    server: str,
    port: int,
    user: str,
    password: str,
    database_name: str,
    schema: str,
    output_file_name: str,
):
    """MSSQL Database Report

    Generate an excel report summarising the data in an MSSQL Database

    Args:
        server (str): The SQL server name
        port (int): The SQL server port
        user (str): Your SQL Server username
        password (str): Your SQL Server password
        database_name (str): The name of the database to analyse
        schema (str): The database schema to analyse
        output_file_name (str): The output file name of the report
    """

    if not output_file_name.endswith(".xlsx"):
        output_file_name = "{}.xlsx".format(output_file_name.split(".")[0])

    with MSSQLConnectionX(server, port, user, password, database_name) as cnx:
        objects = (
            cx.read_sql(
                cnx,
                f"""
SELECT DISTINCT
	('[' + s.name + '].[' + t.name + ']') [TABLE_NAME]
FROM 
    sys.tables t
INNER JOIN 
    sys.partitions p ON t.object_id = p.OBJECT_ID 
INNER JOIN
	sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND s.name = '{schema}'
    AND t.is_ms_shipped = 0
    AND p.rows != 0
                """,
                return_type="polars",
            )
            .select(pl.col("TABLE_NAME"))
            .to_series()
            .to_list()
        )

        build_mssql_field_report(output_file_name, objects, cnx)


@app.command()
def MySQL_Database_Report(
    server: str,
    port: int,
    user: str,
    password: str,
    database_name: str,
    output_file_name: str,
):
    """MySQL Database Report

    Generate an excel report summarising the data in an MySQL Database

    Args:
        server (str): The SQL server name
        port (int): The SQL server port
        user (str): Your SQL Server username
        password (str): Your SQL Server password
        database_name (str): The name of the database to analyse
        output_file_name (str): The output file name of the report
    """

    if not output_file_name.endswith(".xlsx"):
        output_file_name = "{}.xlsx".format(output_file_name.split(".")[0])

    with MySQLConnection(server, port, user, password, database_name) as conn:
        objects = pd.read_sql(
            text(
                "select DISTINCT(CONCAT('`', TABLE_SCHEMA , '`.`' , TABLE_NAME , '`')) \"TABLE_NAME\" from information_schema.columns where table_schema = '{}';".format(
                    database_name
                )
            ),
            conn,
        )["TABLE_NAME"].to_list()
        build_sql_field_report(output_file_name, objects, conn)


if __name__ == "__main__":
    app()
