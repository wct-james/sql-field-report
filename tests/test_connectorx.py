import os
import uuid

from dotenv import load_dotenv

from sql_field_report.sql_field_report import (
    build_dataframe_field_report,
    get_mssql_data,
)
from sql_field_report.utils.databases import MSSQLConnectionX

load_dotenv()


def test_cnx():
    with MSSQLConnectionX(
        os.getenv("SERVER"),
        os.getenv("PORT"),
        os.getenv("USER"),
        os.getenv("SQL_SERVER_PASSWORD"),
        os.getenv("DATABASE"),
    ) as cnx:
        get_mssql_data("[dbo].[Contact_Document]", cnx)


def test_analysis():
    with MSSQLConnectionX(
        os.getenv("SERVER"),
        os.getenv("PORT"),
        os.getenv("USER"),
        os.getenv("SQL_SERVER_PASSWORD"),
        os.getenv("DATABASE"),
    ) as cnx:
        file = f"Test_Report{str(uuid.uuid4())}.xlsx"
        build_dataframe_field_report(
            os.path.join("test_output", file), ["Contact_Document"], get_mssql_data, cnx
        )
        assert file in os.listdir("test_output")

        os.remove(os.path.join("test_output", file))
