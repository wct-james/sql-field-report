import logging
from typing import Callable

import coloredlogs
import pandas as pd
import polars as pl
import typer
from sqlalchemy import Connection, text

from .utils.analysis import analyze_polars_dataframes, analyze_sql_tables
from .utils.databases import MSSQLConnection, MySQLConnection
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


def build_dataframe_field_report(
    output_file_name: str, objects: list, get_data: Callable[[str], pl.DataFrame]
):
    """Build DataFrames Field Report

    Args:
        output_file_name (str): The output file name for the report
        objects (list): A list of tables to be analyzed
        get_data (Callable[[str], pd.DataFrame]): A function that will take in a table name and return a Dataframe

    Returns:
        str: SQL Report filepath
    """

    analysis = analyze_polars_dataframes(objects, get_data)

    path = generate_excel_report(analysis, output_file_name)

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

    with MSSQLConnection(server, port, user, password, database_name) as conn:
        objects = pd.read_sql(
            text(
                "SELECT DISTINCT('[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']') [TABLE_NAME] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{}'".format(
                    schema
                )
            ),
            conn,
        )["TABLE_NAME"].to_list()
        build_sql_field_report(output_file_name, objects, conn)


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
