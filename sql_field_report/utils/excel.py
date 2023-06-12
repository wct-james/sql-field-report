import logging

import pandas as pd
from openpyxl.styles import Alignment, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

logger = logging.getLogger(__name__)


def generate_excel_report(analysis: pd.DataFrame, file_path: str) -> str:
    """Generate an excel data report

    Args:
        analysis (pd.DataFrame): the analysis dataframe
        filepath (str): the filepath to the produced excel

    Returns:
        str: the filepath of the produced excel
    """

    logger.info("Generating Excel Report...")

    try:
        oddFill = PatternFill(
            start_color="DCE6F1", end_color="DCE6F1", fill_type="solid"
        )
        evenFill = PatternFill(
            start_color="B8CCE4", end_color="B8CCE4", fill_type="solid"
        )

        with pd.ExcelWriter(file_path, engine="openpyxl") as xlsx:
            analysis.to_excel(xlsx, "Field_Report")

            ws = xlsx.sheets["Field_Report"]

            ws.delete_cols(1, 1)

            field_report_table = Table(
                displayName="FieldReport",
                ref="A1:{col}{row}".format(
                    col=get_column_letter(ws.max_column), row=ws.max_row
                ),
            )

            style = TableStyleInfo(
                name="TableStyleMedium9",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=False,
                showColumnStripes=False,
            )

            field_report_table.tableStyleInfo = style

            ws.column_dimensions["A"].width = 30
            ws.column_dimensions["B"].width = 50
            ws.column_dimensions["C"].width = 10
            ws.column_dimensions["D"].width = 10
            ws.column_dimensions["E"].width = 10
            ws.column_dimensions["F"].width = 20
            ws.column_dimensions["G"].width = 100

            even = False
            for i, row in enumerate(ws.iter_rows()):
                ws["A{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["B{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["C{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["D{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["E{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["F{}".format(i + 1)].alignment = Alignment(vertical="top")
                ws["G{}".format(i + 1)].alignment = Alignment(wrapText=True)

                if i + 1 == 1:
                    pass
                else:
                    if ws["A{}".format(i + 1)].value != ws["A{}".format(i)].value:
                        even = not even
                    for cell in row:
                        cell.fill = evenFill if even else oddFill

            ws.add_table(field_report_table)

        logger.info("Excel Report Generated")

        return file_path

    except Exception as e:
        logger.error(f"Excel Report Generation Failed: {e}")
        return None
