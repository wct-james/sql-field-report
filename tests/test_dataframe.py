import os

import pandas as pd
import polars as pl
from dotenv import load_dotenv
from sqlalchemy import text

from sql_field_report import build_dataframe_field_report, build_sql_field_report
from sql_field_report.utils.databases import MSSQLConnection
from sql_field_report.utils.file_utils import check_encoding, read_file

load_dotenv()


def test_polars_report():
    files = list([os.path.join("data", i) for i in os.listdir("data")])

    result = build_dataframe_field_report(
        os.path.join("test_output", "Field_Report.xlsx"), files, read_file
    )

    assert result != ""


def read_file_with_kwargs(file: str, custom_arg: str) -> pl.DataFrame:
    if file.endswith(custom_arg):
        encoding = check_encoding(file)
        return pl.read_csv(file, encoding=encoding, infer_schema_length=1000000)
    else:
        return pl.read_excel(file)


def test_polars_report_with_kwargs():
    files = list([os.path.join("data", i) for i in os.listdir("data")])

    result = build_dataframe_field_report(
        os.path.join("test_output", "Field_Report.xlsx"),
        files,
        read_file_with_kwargs,
        custom_arg=".csv",
    )

    assert result != ""


def test_sql_report():
    with MSSQLConnection(
        os.getenv("SERVER"),
        int(os.getenv("PORT")),
        os.getenv("USER"),
        os.getenv("SQL_SERVER_PASSWORD"),
        os.getenv("DATABASE"),
    ) as conn:
        objects = pd.read_sql(
            text(
                "SELECT DISTINCT('[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']') [TABLE_NAME] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{}'".format(
                    "dbo"
                )
            ),
            conn,
        )["TABLE_NAME"].to_list()[:2]
        build_sql_field_report(
            os.path.join("test_output", "SQLReport.xlsx"), objects, conn
        )
