import os
from sql_field_report import build_dataframe_field_report
from pathlib import Path
import cchardet as chardet
import pandas as pd

def check_encoding(filename: str):
    filepath = Path(filename)

    blob = filepath.read_bytes()
    detection = chardet.detect(blob)

    encoding = detection.get("encoding")

    return encoding


def read_file(file: str) -> pd.DataFrame:
    if file.endswith(".csv"):
        encoding = check_encoding(file)
        return pd.read_csv(file, encoding=encoding, low_memory=False)
    else:
        return pd.read_excel(file)


def test_dataframe_field_report():
    files = list([os.path.join('data', i) for i in os.listdir('data')])

    build_dataframe_field_report(os.path.join('test_output', 'Field_Report.xlsx'), files, read_file)
