import os

import pandas as pd
from sqlalchemy import text

from sql_field_report import build_dataframe_field_report, build_sql_field_report
from sql_field_report.utils.databases import MSSQLConnection
from sql_field_report.utils.file_utils import read_file


def test_polars_report():
    files = list([os.path.join("data", i) for i in os.listdir("data")])

    build_dataframe_field_report(
        os.path.join("test_output", "Field_Report.xlsx"), files, read_file
    )


def test_sql_report():
    with MSSQLConnection(
        "10.64.0.31", 1433, "sa", "yourStrong(!)Password", "LLCP"
    ) as conn:
        objects = pd.read_sql(
            text(
                "SELECT DISTINCT('[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']') [TABLE_NAME] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{}'".format(
                    "dbo"
                )
            ),
            conn,
        )["TABLE_NAME"].to_list()
        build_sql_field_report(
            os.path.join("test_output", "SQLReport.xlsx"), objects, conn
        )
