from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc, row_number
from pyspark.sql.window import Window


# === Initialize Spark ===
sc = SparkContext()
spark = SparkSession(sc)

# === Load paths from Airflow Variables ===
bucket = "project3dt"
songs_path = "raw_data/songs/"
users_path = "raw_data/users/"
streams_path = "raw_data/streams/"

kpi_output_path = "processed/genre_kpis"
top_songs_output_path = "processed/top_songs"
top_genres_output_path = "processed/top_genres"

# === 1. Read input from S3 ===
songs_df = spark.read.option("header", True).csv(f"s3://{bucket}/{songs_path}")  # replace with your actual db and table name
users_df = spark.read.option("header", True).csv(f"s3://{bucket}/{users_path}") # replace with your actual db and table name
streams_df = spark.read.option("header", True).csv(f"s3://{bucket}/{streams_path}")

# === 3. Safe column renaming and casting ===
songs_df = songs_df.withColumnRenamed("track_genre", "genre") \
                   .withColumnRenamed("duration_ms", "duration") \
                   .withColumn("duration", col("duration").cast("int"))

streams_df = streams_df.withColumn("listen_time", col("listen_time").cast("timestamp"))

# === 4. Join datasets ===
df = streams_df.join(songs_df, on="track_id", how="inner") \
               .join(users_df, on="user_id", how="inner")

# === 5. Add date column ===
df = df.withColumn("date", col("listen_time").substr(0, 10))

# === 6. KPI Calculations ===

# Genre KPIs
genre_kpi = df.groupBy("genre", "date").agg(
    count("track_id").alias("listen_count"),
    count("user_id").alias("unique_listeners"),
    _sum("duration").alias("total_listen_time"),
    avg("duration").alias("avg_listen_time_per_user")
)

