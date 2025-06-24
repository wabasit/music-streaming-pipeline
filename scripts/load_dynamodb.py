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

