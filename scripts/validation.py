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


# --- NULL VALIDATION ---
def check_nulls(df: DataFrame, critical_columns: List[str], name: str):
    for column in critical_columns:
        field_type = [f.dataType for f in df.schema.fields if f.name == column][0]
        
        # For numeric types, check for both null and NaN
        if isinstance(field_type, NumericType):
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        else:
            null_count = df.filter(col(column).isNull()).count()
        
        if null_count > 0:
            print(f"[{name}] Column '{column}' has {null_count} null or NaN values.")
        else:
            print(f"[{name}] Column '{column}' passed null/NaN check.")

# --- DUPLICATE VALIDATION ---
def check_duplicates(df: DataFrame, subset: List[str], name: str):
    dup_count = df.groupBy(subset).count().filter(col("count") > 1).count()
    if dup_count > 0:
        print(f"[{name}] Found {dup_count} duplicate records based on {subset}.")
    else:
        print(f"[{name}] No duplicates in {subset}.")

# --- RANGE / LOGIC VALIDATION ---
def check_range(df: DataFrame, column: str, min_val: int = None, max_val: int = None, name: str = ""):
    if min_val is not None:
        below_min = df.filter(col(column) < min_val).count()
        if below_min > 0:
            print(f"[{name}] {below_min} values in '{column}' below {min_val}.")
    if max_val is not None:
        above_max = df.filter(col(column) > max_val).count()
        if above_max > 0:
            print(f"[{name}] {above_max} values in '{column}' above {max_val}.")

# --- REFERENTIAL INTEGRITY CHECK ---
def check_foreign_keys(df: DataFrame, ref_df: DataFrame, key: str, df_name: str, ref_name: str):
    invalid = df.join(ref_df, key, "left_anti")
    if invalid.count() > 0:
        print(f"[{df_name}] Contains {invalid.count()} records with invalid '{key}' not found in {ref_name}.")
    else:
        print(f"[{df_name}] All '{key}' values exist in {ref_name}.")

# --- ENTRYPOINT FUNCTION ---
def run_validations(songs_df: DataFrame, users_df: DataFrame, streams_df: DataFrame):
    print("\n=== RUNNING VALIDATIONS ===")

    # 1. SCHEMA CHECKS
    songs_df = songs_df.withColumn("duration_ms", col("duration_ms").cast("int"))
    users_df = users_df.withColumn("user_age", col("user_age").cast("int"))
    streams_df = streams_df.withColumn("listen_time", col("listen_time").cast("timestamp"))
    validate_schema(songs_df, {"track_id": "string", "track_genre": "string", "duration_ms": "int"}, "Songs")
    validate_schema(users_df, {"user_id": "string", "user_age": "int", "user_country": "string"}, "Users")
    validate_schema(streams_df, {"user_id": "string", "track_id": "string", "listen_time": "timestamp"}, "Streams")

    # Flag and save bad rows
    songs_df = flag_and_save_bad_rows(songs_df, ["track_id", "track_genre", "duration_ms"], "Songs", "bad_rows/songs_bad_rows.csv")
    users_df = flag_and_save_bad_rows(users_df, ["user_id", "user_age", "user_country"], "Users", "bad_rows/users_bad_rows.csv")
    streams_df = flag_and_save_bad_rows(streams_df, ["user_id", "track_id", "listen_time"], "Streams", "bad_rows/streams_bad_rows.csv")
    
    