import boto3
import csv
from io import StringIO

# Initialize AWS clients (real AWS environment)
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

# Config
bucket = "project3dt"

# Tables and their configs
tables_config = {
    "genre_kpi_table": {
        "name": "genre_kpi",
        "hash_key": ("genre", "S"),
        "range_key": ("date", "S"),
        "s3_prefix": "processed/genre_kpis/"
    },
    "top_songs_table": {
        "name": "top_songs",
        "hash_key": ("date#genre", "S"),
        "range_key": ("rank", "N"),
        "s3_prefix": "processed/top_songs/"
    },
    "top_genres_table": {
        "name": "top_genres",
        "hash_key": ("date", "S"),
        "range_key": ("rank", "N"),
        "s3_prefix": "processed/top_genres/"
    }
}

# Create table if it does not exist
def ensure_table_exists(table_name, hash_key, range_key=None):
    existing_tables = dynamodb_client.list_tables()["TableNames"]
    if table_name in existing_tables:
        print(f" Table '{table_name}' exists.")
        return

    key_schema = [{"AttributeName": hash_key[0], "KeyType": "HASH"}]
    attr_definitions = [{"AttributeName": hash_key[0], "AttributeType": hash_key[1]}]

    if range_key:
        key_schema.append({"AttributeName": range_key[0], "KeyType": "RANGE"})
        attr_definitions.append({"AttributeName": range_key[0], "AttributeType": range_key[1]})

    print(f" Creating table '{table_name}'...")
    dynamodb_client.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attr_definitions,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    waiter = dynamodb_client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)
    print(f"Table '{table_name}' created.")

# Load CSV rows from S3 and yield dicts
def load_csv_from_s3(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                print(f"Reading: {key}")
                response = s3.get_object(Bucket=bucket, Key=key)
                content = response["Body"].read().decode("utf-8")
                reader = csv.DictReader(StringIO(content))

                if reader.fieldnames is None:
                    print(f"Skipped file with no header: {key}")
                    continue

                # Debug: Print column names for troubleshooting
                print(f"CSV columns in {key}: {reader.fieldnames}")

                for row in reader:
                    yield row, key  # Also return the filename for debugging

# Convert value to int safely, raising an error if conversion fails
def safe_int(value, field_name, table_name, filename=""):
    if value in (None, '', 'NULL', 'null'):
        return None
    try:
        return int(float(value))  # convert to float first, then int
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid number '{value}' for field '{field_name}' in table '{table_name}' from file '{filename}': {e}")

try:
    # Load data to DynamoDB
    for config in tables_config.values():
        ensure_table_exists(config["name"], config["hash_key"], config["range_key"])
        table = dynamodb.Table(config["name"])
        
        print(f"\nProcessing table: {config['name']}")
        
        for item, filename in load_csv_from_s3(config["s3_prefix"]):
            try:
                # Debug: Print first few items to understand data structure
                print(f"Sample item from {filename}: {dict(list(item.items())[:3])}")
                
                # Process based on table type
                if config["name"] == "top_songs":
                    # Validate required fields exist
                    if "date" not in item or "genre" not in item or "rank" not in item:
                        print(f"Missing required fields in item: {item}")
                        continue
                    
                    item["date#genre"] = f"{item['date']}#{item['genre']}"
                    rank_value = safe_int(item["rank"], "rank", config["name"], filename)
                    if rank_value is None:
                        print(f"Skipping item with null rank: {item}")
                        continue
                    item["rank"] = rank_value
                    
                elif config["name"] == "top_genres":
                    # Validate required fields exist
                    if "date" not in item or "rank" not in item:
                        print(f"Missing required fields in item: {item}")
                        continue
                    
                    rank_value = safe_int(item["rank"], "rank", config["name"], filename)
                    if rank_value is None:
                        print(f"Skipping item with null rank: {item}")
                        continue
                    item["rank"] = rank_value
                
                # Skip items with missing required keys or empty values for critical fields
                critical_fields = ["date", "rank"] if config["name"] != "genre_kpi" else ["genre", "date"]
                if any(item.get(field) in (None, '', 'NULL', 'null') for field in critical_fields):
                    print(f"Skipping item with missing critical fields: {item}")
                    continue

                # Clean up empty string values (convert to None or remove)
                cleaned_item = {}
                for k, v in item.items():
                    if v in ('', 'NULL', 'null'):
                        cleaned_item[k] = None
                    else:
                        cleaned_item[k] = v

                table.put_item(Item=cleaned_item)
                
            except Exception as item_error:
                print(f"Error processing item {item} from {filename}: {item_error}")
                # Continue processing other items instead of failing completely
                continue

    print("DynamoDB Load Complete")

except Exception as e:
    print(f"Load Failed: {e}")
    import traceback
    print(f"Full traceback: {traceback.format_exc()}")
    raise e  # This will fail the Glue job properly