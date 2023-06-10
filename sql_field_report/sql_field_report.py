from typing import Callable

import pandas as pd
import typer
from sqlalchemy import text
from utils.analysis import analyze_dataframes, analyze_sql_tables
from utils.databases import MSSQLConnection
from utils.excel import generate_excel_report


def build_sql_field_report(output_file_name: str, objects: list, schema: str, conn):
    """Build SQL Field Report

    Args:
        output_file_name (str): The output file name for the report
        objects (list): A list of tables to be analyzed
        schema (str): The schema to analyze
        conn: SQLAlchemy connection

    Returns:
        str: SQL Report filepath
    """

    analysis = analyze_sql_tables(objects, schema, conn)

    path = generate_excel_report(analysis, output_file_name)

    if path:
        return path
    else:
        return None


def build_dataframe_field_report(
    output_file_name: str, objects: list, get_data: Callable[[str], pd.DataFrame]
):
    """Build DataFrames Field Report

    Args:
        output_file_name (str): The output file name for the report
        objects (list): A list of tables to be analyzed
        get_data (Callable[[str], pd.DataFrame]): A function that will take in a table name and return a Dataframe

    Returns:
        str: SQL Report filepath
    """

    analysis = analyze_dataframes(objects, get_data)

    path = generate_excel_report(analysis, output_file_name)

    if path:
        return path
    else:
        return None


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
        schema (str): The schema for the database tables to analyse
        output_file_name (str): The output file name of the report
    """

    if not output_file_name.endswith(".xlsx"):
        output_file_name = "{}.xlsx".format(output_file_name.split(".")[0])

    with MSSQLConnection(server, port, user, password, database_name) as conn:
        objects = pd.read_sql(
            text("SELECT DISTINCT(TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS"), conn
        )["TABLE_NAME"].to_list()
        build_sql_field_report(output_file_name, objects, schema, conn)


if __name__ == "__main__":
    app()
