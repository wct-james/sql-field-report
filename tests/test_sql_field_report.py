import uuid
from os import getenv, listdir, remove
from os.path import join

from dotenv import load_dotenv

from sql_field_report.sql_field_report import MSSQL_Database_Report

load_dotenv()


def test_mssql_field_report():
    file = f"Test_Report{str(uuid.uuid4())}.xlsx"
    MSSQL_Database_Report(
        getenv("SERVER"),
        getenv("PORT"),
        getenv("USER"),
        getenv("SQL_SERVER_PASSWORD"),
        getenv("DATABASE"),
        "dbo",
        join("test_output", file),
    )

    assert file in listdir("test_output")

    remove(join("test_output", file))
