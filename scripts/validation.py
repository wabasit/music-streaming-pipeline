# validation.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when, lit
from typing import Dict, List
from pyspark.sql.functions import col, isnan
from pyspark.sql.types import FloatType, DoubleType, NumericType, TimestampType

# --- SCHEMA VALIDATION ---
def validate_schema(df: DataFrame, expected_cols: Dict[str, str], name: str):
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    for col_name, data_type in expected_cols.items():
        if col_name not in actual_schema:
            raise Exception(f"[{name}] Missing column: {col_name}")
        if actual_schema[col_name] != data_type:
            raise Exception(f"[{name}] Column '{col_name}' expected type '{data_type}' but got '{actual_schema[col_name]}'")
    print(f"[{name}] Schema validation passed.")

