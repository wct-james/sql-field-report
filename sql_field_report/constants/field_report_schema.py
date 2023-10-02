"""Contains definitions of the field report schema"""

TABLE_FILE = "Table/File"
FIELD = "Field"
COUNT = "Count"
POPULATED = "Populated"
UNIQUE = "Unique"
DATATYPE = "Datatype"
TOP_VALUES = "Top Values"
CHOICES = "Choices"

FIELD_REPORT_SCHEMA = [
    TABLE_FILE,
    FIELD,
    COUNT,
    POPULATED,
    UNIQUE,
    DATATYPE,
    TOP_VALUES,
    CHOICES,
]

# CHOICE MAPPING SCHEMA
LEGACY = "Legacy Value"
TARGET = "Target Value"

MAPPING_TABLE_COLUMNS_COUNT = 5
LEGACY_COL_IDX = 0
TARGET_COL_IDX = 1
OPTIONS_COL_IDX = 3


def calculate_col_index_for_mapping_table(
    mapping_table_count: int, col_type: int
) -> int:
    """
    Return the column index number for a legacy mapping column
    Args:
        col_type: 0("LEGACY") or 1("TARGET") or 3("OPTIONS)
        mapping_table_count: the number of mapping tables already in the sheet

    Returns:
        index of desired mapping table column
    """
    if col_type not in [0, 1, 3]:
        raise IndexError(
            "Column type must be one of: [LEGACY_COL_IDX, TARGET_COL_IDX, OPTIONS_COL_IDX]"
        )
    start_idx = mapping_table_count * MAPPING_TABLE_COLUMNS_COUNT
    return start_idx + col_type + 1
