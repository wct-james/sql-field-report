from pathlib import Path

import cchardet as chardet
import pandas as pd
import polars as pl


def check_encoding(filename: str):
    filepath = Path(filename)

    blob = filepath.read_bytes()
    detection = chardet.detect(blob)

    encoding = detection.get("encoding")

    return encoding


def read_file_pandas(file: str) -> pd.DataFrame:
    if file.endswith(".csv"):
        encoding = check_encoding(file)
        return pd.read_csv(file, encoding=encoding, low_memory=False)
    else:
        return pd.read_excel(file)


def read_file(file: str) -> pl.DataFrame:
    if file.endswith(".csv"):
        encoding = check_encoding(file)
        return pl.read_csv(file, encoding=encoding, infer_schema_length=0)
    else:
        return pl.read_excel(file, read_csv_options={"infer_schema_length": 0})
