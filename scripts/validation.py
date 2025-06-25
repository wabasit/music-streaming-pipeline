# validation.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, isnan, when, lit
from typing import Dict, List
from pyspark.sql.functions import col, isnan
from pyspark.sql.types import FloatType, DoubleType, NumericType, TimestampType, IntegerType
import sys

# Initialize Spark if not already done
try:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("DataValidation").getOrCreate()
except Exception as e:
    print(f"Error initializing Spark: {e}")
    sys.exit(1)

# --- SCHEMA VALIDATION ---
def validate_schema(df: DataFrame, expected_cols: Dict[str, str], name: str):
    """Validate that DataFrame has expected columns with correct data types"""
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    
    missing_cols = []
    type_mismatches = []
    
    for col_name, expected_type in expected_cols.items():
        if col_name not in actual_schema:
            missing_cols.append(col_name)
        elif actual_schema[col_name] != expected_type:
            type_mismatches.append(f"'{col_name}' expected '{expected_type}' but got '{actual_schema[col_name]}'")
    
    if missing_cols:
        raise Exception(f"[{name}] Missing columns: {missing_cols}")
    if type_mismatches:
        raise Exception(f"[{name}] Type mismatches: {type_mismatches}")
    
    print(f"[{name}] Schema validation passed.")
    return True

# --- FLAG AND SAVE BAD ROWS ---
def flag_and_save_bad_rows(df: DataFrame, critical_columns: list, name: str, output_path: str):
    """Identify bad rows and save them separately, return clean DataFrame"""
    try:
        # Get the schema to identify which columns are numeric
        schema = dict(df.dtypes)
        
        # Build filter condition for bad rows
        conditions = []
        for column in critical_columns:
            if column not in schema:
                print(f"[{name}] Warning: Column '{column}' not found in DataFrame")
                continue
                
            # Check for null values
            is_null = col(column).isNull()
            
            # For numeric types, also check for NaN
            if schema[column] in ["double", "float"]:
                conditions.append(is_null | isnan(col(column)))
            else:
                # For string columns, also check for empty strings
                if schema[column] == "string":
                    conditions.append(is_null | (col(column) == ""))
                else:
                    conditions.append(is_null)
        
        if not conditions:
            print(f"[{name}] No conditions to check for bad rows")
            return df
        
        # Combine all conditions with OR
        bad_condition = conditions[0]
        for condition in conditions[1:]:
            bad_condition = bad_condition | condition
        
        bad_rows = df.filter(bad_condition)
        good_rows = df.filter(~bad_condition)
        
        bad_count = bad_rows.count()
        good_count = good_rows.count()
        total_count = df.count()
        
        print(f"[{name}] Total rows: {total_count}, Good rows: {good_count}, Bad rows: {bad_count}")
        
        if bad_count > 0:
            print(f"[{name}] Saving {bad_count} bad rows to s3://project3dt/validation/{output_path}")
            bad_rows.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://project3dt/validation/{output_path}")
            
            if bad_count > 10:
                print(f"[{name}] Showing first 10 bad rows:")
                bad_rows.show(10, truncate=False)
        else:
            print(f"[{name}] No bad rows found.")
        
        return good_rows
        
    except Exception as e:
        print(f"[{name}] Error in flag_and_save_bad_rows: {str(e)}")
        return df

# --- NULL VALIDATION ---
def check_nulls(df: DataFrame, critical_columns: List[str], name: str):
    """Check for null and NaN values in critical columns"""
    for column in critical_columns:
        if column not in dict(df.dtypes):
            print(f"[{name}] Warning: Column '{column}' not found")
            continue
            
        try:
            field_type = [f.dataType for f in df.schema.fields if f.name == column][0]
            
            # For numeric types, check for both null and NaN
            if isinstance(field_type, (NumericType, IntegerType, FloatType, DoubleType)):
                null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            else:
                null_count = df.filter(col(column).isNull()).count()
            
            total_count = df.count()
            if null_count > 0:
                percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                print(f"[{name}] Column '{column}' has {null_count} null/NaN values ({percentage:.2f}%)")
            else:
                print(f"[{name}] Column '{column}' passed null/NaN check.")
                
        except Exception as e:
            print(f"[{name}] Error checking nulls for column '{column}': {str(e)}")

# --- DUPLICATE VALIDATION ---
def check_duplicates(df: DataFrame, subset: List[str], name: str):
    """Check for duplicate records based on specified columns"""
    try:
        # Check if all columns exist
        missing_cols = [col for col in subset if col not in dict(df.dtypes)]
        if missing_cols:
            print(f"[{name}] Warning: Columns {missing_cols} not found for duplicate check")
            return
        
        total_count = df.count()
        distinct_count = df.select(subset).distinct().count()
        duplicate_count = total_count - distinct_count
        
        if duplicate_count > 0:
            print(f"[{name}] Found {duplicate_count} duplicate records based on {subset}")
            
            # Show sample duplicates
            duplicates = df.groupBy(subset).count().filter(col("count") > 1)
            if duplicates.count() > 0:
                print(f"[{name}] Sample duplicate groups:")
                duplicates.show(5)
        else:
            print(f"[{name}] No duplicates found based on {subset}")
            
    except Exception as e:
        print(f"[{name}] Error in duplicate check: {str(e)}")

# --- RANGE / LOGIC VALIDATION ---
def check_range(df: DataFrame, column: str, min_val: int = None, max_val: int = None, name: str = ""):
    """Check if values in column are within specified range"""
    try:
        if column not in dict(df.dtypes):
            print(f"[{name}] Warning: Column '{column}' not found for range check")
            return
        
        total_count = df.count()
        
        if min_val is not None:
            below_min = df.filter(col(column) < min_val).count()
            if below_min > 0:
                percentage = (below_min / total_count) * 100 if total_count > 0 else 0
                print(f"[{name}] {below_min} values ({percentage:.2f}%) in '{column}' below {min_val}")
            else:
                print(f"[{name}] All values in '{column}' are >= {min_val}")
        
        if max_val is not None:
            above_max = df.filter(col(column) > max_val).count()
            if above_max > 0:
                percentage = (above_max / total_count) * 100 if total_count > 0 else 0
                print(f"[{name}] {above_max} values ({percentage:.2f}%) in '{column}' above {max_val}")
            else:
                print(f"[{name}] All values in '{column}' are <= {max_val}")
                
    except Exception as e:
        print(f"[{name}] Error in range check for '{column}': {str(e)}")

# --- REFERENTIAL INTEGRITY CHECK ---
def check_foreign_keys(df: DataFrame, ref_df: DataFrame, key: str, df_name: str, ref_name: str):
    """Check referential integrity between DataFrames"""
    try:
        if key not in dict(df.dtypes):
            print(f"[{df_name}] Warning: Key column '{key}' not found")
            return
        if key not in dict(ref_df.dtypes):
            print(f"[{ref_name}] Warning: Key column '{key}' not found")
            return
        
        # Find records in df that don't have matching keys in ref_df
        invalid = df.join(ref_df.select(key).distinct(), key, "left_anti")
        invalid_count = invalid.count()
        total_count = df.count()
        
        if invalid_count > 0:
            percentage = (invalid_count / total_count) * 100 if total_count > 0 else 0
            print(f"[{df_name}] Contains {invalid_count} records ({percentage:.2f}%) with invalid '{key}' not found in {ref_name}")
            
            # Show sample invalid keys
            print(f"[{df_name}] Sample invalid {key} values:")
            invalid.select(key).distinct().show(10)
        else:
            print(f"[{df_name}] All '{key}' values exist in {ref_name}")
            
    except Exception as e:
        print(f"Error in foreign key check between {df_name} and {ref_name}: {str(e)}")

# --- ENTRYPOINT FUNCTION ---
def run_validations(songs_df: DataFrame, users_df: DataFrame, streams_df: DataFrame):
    """Main validation function that runs all checks"""
    print("\n" + "="*50)
    print("RUNNING DATA VALIDATIONS")
    print("="*50)
    
    try:
        # Print initial counts
        print(f"Initial counts - Songs: {songs_df.count()}, Users: {users_df.count()}, Streams: {streams_df.count()}")
        
        # 1. DATA TYPE CASTING AND SCHEMA CHECKS
        print("\n--- STEP 1: SCHEMA VALIDATION ---")
        try:
            songs_df = songs_df.withColumn("duration_ms", col("duration_ms").cast("int"))
            users_df = users_df.withColumn("user_age", col("user_age").cast("int"))
            streams_df = streams_df.withColumn("listen_time", col("listen_time").cast("timestamp"))
            
            validate_schema(songs_df, {"track_id": "string", "track_genre": "string", "duration_ms": "int"}, "Songs")
            validate_schema(users_df, {"user_id": "string", "user_age": "int", "user_country": "string"}, "Users")
            validate_schema(streams_df, {"user_id": "string", "track_id": "string", "listen_time": "timestamp"}, "Streams")
        except Exception as e:
            print(f"Schema validation failed: {str(e)}")
            raise
        
        # 2. FLAG AND SAVE BAD ROWS
        print("\n--- STEP 2: CLEANING BAD ROWS ---")
        songs_df_clean = flag_and_save_bad_rows(songs_df, ["track_id", "track_genre", "duration_ms"], "Songs", "songs_bad_rows")
        users_df_clean = flag_and_save_bad_rows(users_df, ["user_id", "user_age", "user_country"], "Users", "users_bad_rows")
        streams_df_clean = flag_and_save_bad_rows(streams_df, ["user_id", "track_id", "listen_time"], "Streams", "streams_bad_rows")
        
        # Print cleaned counts
        print(f"Cleaned counts - Songs: {songs_df_clean.count()}, Users: {users_df_clean.count()}, Streams: {streams_df_clean.count()}")
        
        # 3. NULL CHECKS
        print("\n--- STEP 3: NULL VALIDATION ---")
        check_nulls(songs_df_clean, ["track_id", "track_genre", "duration_ms"], "Songs")
        check_nulls(users_df_clean, ["user_id", "user_age", "user_country"], "Users")
        check_nulls(streams_df_clean, ["user_id", "track_id", "listen_time"], "Streams")
        
        # 4. DUPLICATE CHECKS
        print("\n--- STEP 4: DUPLICATE VALIDATION ---")
        check_duplicates(users_df_clean, ["user_id"], "Users")
        check_duplicates(songs_df_clean, ["track_id"], "Songs")
        check_duplicates(streams_df_clean, ["user_id", "track_id", "listen_time"], "Streams")
        
        # 5. RANGE CHECKS
        print("\n--- STEP 5: RANGE VALIDATION ---")
        check_range(songs_df_clean, "duration_ms", min_val=1000, name="Songs")  # At least 1 second
        check_range(users_df_clean, "user_age", min_val=13, max_val=120, name="Users")  # Reasonable age range
        
        # 6. FOREIGN KEY CHECKS
        print("\n--- STEP 6: REFERENTIAL INTEGRITY ---")
        check_foreign_keys(streams_df_clean, users_df_clean, "user_id", "Streams", "Users")
        check_foreign_keys(streams_df_clean, songs_df_clean, "track_id", "Streams", "Songs")
        
        print("\n" + "="*50)
        print("ALL VALIDATIONS COMPLETE")
        print("="*50)
        
        return songs_df_clean, users_df_clean, streams_df_clean
        
    except Exception as e:
        print(f"Validation failed with error: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        raise e

# --- MAIN EXECUTION FOR GLUE JOB ---
if __name__ == "__main__":
    try:
        # This would be used when running as a standalone Glue job
        bucket = "project3dt"
        
        # Read data
        songs_df = spark.read.option("header", True).csv(f"s3://{bucket}/raw_data/songs/")
        users_df = spark.read.option("header", True).csv(f"s3://{bucket}/raw_data/users/")
        streams_df = spark.read.option("header", True).csv(f"s3://{bucket}/raw_data/streams/")
        
        # Run validations
        songs_clean, users_clean, streams_clean = run_validations(songs_df, users_df, streams_df)
        
        # Save cleaned data
        songs_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/validated/songs/")
        users_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/validated/users/")
        streams_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/validated/streams/")
        
        print("Validation job completed successfully!")
        
    except Exception as e:
        print(f"Validation job failed: {str(e)}")
        raise e