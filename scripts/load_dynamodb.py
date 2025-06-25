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
                for row in reader:
                    yield row

# Convert value to int safely, raising an error if conversion fails
def safe_int(value, field_name, table_name):
    try:
        return int(float(value))  # convert to float first, then int
    except ValueError as e:
        raise ValueError(f"Invalid number '{value}' for field '{field_name}' in table '{table_name}': {e}")
    
# Load data to DynamoDB
for config in tables_config.values():
    ensure_table_exists(config["name"], config["hash_key"], config["range_key"])
    table = dynamodb.Table(config["name"])
    for item in load_csv_from_s3(config["s3_prefix"]):
        # inside your loop:
        if config["name"] == "top_songs":
            item["date#genre"] = f"{item['date']}#{item['genre']}"
            item["rank"] = safe_int(item["rank"], "rank", config["name"])
        elif config["name"] == "top_genres":
            item["rank"] = safe_int(item["rank"], "rank", config["name"])
        table.put_item(Item=item)

print("DynamoDB Load Complete")