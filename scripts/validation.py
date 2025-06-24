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

# --- FLAG AND SAVE BAD ROWS ---
def flag_and_save_bad_rows(df: DataFrame, critical_columns: list, name: str, output_path: str):
    # Get the schema to identify which columns are numeric
    schema = dict(df.dtypes)
    
    # Build filter condition only using isnan on numeric types
    condition = None
    for column in critical_columns:
        is_null = col(column).isNull()
        if schema[column] in ["double", "float"]:
            col_condition = is_null | isnan(col(column))
        else:
            col_condition = is_null
        
        condition = col_condition if condition is None else condition | col_condition

    bad_rows = df.filter(condition)
    good_rows = df.filter(~condition)

    bad_count = bad_rows.count()
    if bad_count > 0:
        print(f"[{name}] Found {bad_count} bad rows. Saving to {output_path}...")
        bad_rows.write.mode("overwrite").option("header", True).csv(output_path)
    else:
        print(f"[{name}] No bad rows found.")

    return good_rows
  # This is the cleaned version to continue with the pipeline


