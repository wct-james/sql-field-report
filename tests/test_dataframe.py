import os

from sql_field_report import build_dataframe_field_report
from sql_field_report.utils.file_utils import read_file


# def test_dataframe_field_report():
#     files = list([os.path.join("data", i) for i in os.listdir("data")])

#     build_dataframe_field_report(
#         os.path.join("test_output", "Field_Report.xlsx"), files, read_file
#     )

def test_polars_report():
    files = list([os.path.join("data", i) for i in os.listdir("data")])

    build_dataframe_field_report(
        os.path.join("test_output", "Field_Report.xlsx"), files, read_file
    )