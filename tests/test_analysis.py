import os
import uuid

import polars as pl

from sql_field_report import build_dataframe_field_report
from sql_field_report.utils.analysis import analyze_data
from sql_field_report.utils.file_utils import read_file


def get_data(name: str) -> pl.DataFrame:
    df = pl.from_records(
        data=(
            (f"{name}1", 1, "A"),
            (f"{name}2", 2, "B"),
            (f"{name}3", 3, "C"),
        ),
        schema=["Name", "Number", "Letter"],
    )

    return df


def test_analyse():
    file = f"Test_Report{str(uuid.uuid4())}.xlsx"
    build_dataframe_field_report(os.path.join("test_output", file), ["test"], get_data)
    assert file in os.listdir("test_output")

    os.remove(os.path.join("test_output", file))


def test_analyze_data():
    analysis = analyze_data(
        r"data\Account.csv",
        read_file,
    )
