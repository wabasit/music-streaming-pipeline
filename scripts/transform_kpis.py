from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc, row_number, when, isnan, isnull
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

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

try:
    # === 1. Read input from S3 ===
    print("Reading data from S3...")
    songs_df = spark.read.option("header", True).csv(f"s3://{bucket}/{songs_path}")
    users_df = spark.read.option("header", True).csv(f"s3://{bucket}/{users_path}")
    streams_df = spark.read.option("header", True).csv(f"s3://{bucket}/{streams_path}")
    
    # === 2. Debug: Print schemas and sample data ===
    print("Songs DataFrame Schema:")
    songs_df.printSchema()
    print(f"Songs count: {songs_df.count()}")
    
    print("Users DataFrame Schema:")
    users_df.printSchema()
    print(f"Users count: {users_df.count()}")
    
    print("Streams DataFrame Schema:")
    streams_df.printSchema()
    print(f"Streams count: {streams_df.count()}")
    
    # === 3. Safe column renaming and casting ===
    print("Processing songs data...")
    songs_df = songs_df.withColumnRenamed("track_genre", "genre") \
                       .withColumnRenamed("duration_ms", "duration") \
                       .withColumn("duration", col("duration").cast("int"))
    
    # Clean null values in songs
    songs_df = songs_df.filter(col("track_id").isNotNull() & col("genre").isNotNull())
    
    print("Processing streams data...")
    streams_df = streams_df.withColumn("listen_time", col("listen_time").cast("timestamp"))
    
    # Clean null values in streams
    streams_df = streams_df.filter(col("track_id").isNotNull() & col("user_id").isNotNull() & col("listen_time").isNotNull())
    
    # Clean null values in users
    users_df = users_df.filter(col("user_id").isNotNull())
    
    # === 4. Join datasets ===
    print("Joining datasets...")
    df = streams_df.join(songs_df, on="track_id", how="inner") \
                   .join(users_df, on="user_id", how="inner")
    
    print(f"Joined data count: {df.count()}")
    if df.count() == 0:
        raise ValueError("No data after joins - check your input data integrity")
    
    # === 5. Add date column ===
    df = df.withColumn("date", col("listen_time").substr(0, 10))
    
    # === 6. KPI Calculations ===
    print("Calculating Genre KPIs...")
    
    # Genre KPIs - Fixed aggregation logic
    genre_kpi = df.groupBy("genre", "date").agg(
        count("track_id").alias("listen_count"),
        count("user_id").alias("unique_listeners"),
        _sum("duration").alias("total_listen_time"),
        avg("duration").alias("avg_listen_time_per_user")
    )
    
    # Clean null values and ensure proper data types
    genre_kpi = genre_kpi.filter(col("genre").isNotNull() & col("date").isNotNull())
    
    print(f"Genre KPI count: {genre_kpi.count()}")
    if genre_kpi.count() > 0:
        print("Sample Genre KPI data:")
        genre_kpi.show(5)
    
    # === Top 3 Songs per Genre per Day ===
    print("Calculating Top Songs...")
    
    # Fixed: Use listen_count instead of duration for ranking
    song_popularity = df.groupBy("track_id", "genre", "date", "track_name").agg(
        count("track_id").alias("play_count")
    )
    
    windowSpec = Window.partitionBy("genre", "date").orderBy(desc("play_count"))
    top_songs = song_popularity.withColumn("rank", row_number().over(windowSpec)) \
                              .filter(col("rank") <= 3) \
                              .select("date", "genre", "track_id", "track_name", "play_count", "rank")
    
    print(f"Top songs count: {top_songs.count()}")
    if top_songs.count() > 0:
        print("Sample Top Songs data:")
        top_songs.show(5)
    
    # === Top 5 Genres per Day ===
    print("Calculating Top Genres...")
    
    # Fixed: Use the genre_kpi data for consistency
    genre_daily_totals = genre_kpi.groupBy("date", "genre").agg(
        _sum("listen_count").alias("total_listens")
    )
    
    genre_window = Window.partitionBy("date").orderBy(desc("total_listens"))
    top_genres = genre_daily_totals.withColumn("rank", row_number().over(genre_window)) \
                                  .filter(col("rank") <= 5) \
                                  .select("date", "genre", "total_listens", "rank")
    
    print(f"Top genres count: {top_genres.count()}")
    if top_genres.count() > 0:
        print("Sample Top Genres data:")
        top_genres.show(5)
    
    # === 7. Data Quality Checks ===
    print("Performing data quality checks...")
    
    # Check for required columns and data types
    required_genre_kpi_cols = ["genre", "date", "listen_count", "unique_listeners"]
    for col_name in required_genre_kpi_cols:
        null_count = genre_kpi.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"WARNING: {null_count} null values in {col_name} for genre_kpi")
    
    required_top_songs_cols = ["date", "genre", "rank"]
    for col_name in required_top_songs_cols:
        null_count = top_songs.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"WARNING: {null_count} null values in {col_name} for top_songs")
    
    required_top_genres_cols = ["date", "genre", "rank"]
    for col_name in required_top_genres_cols:
        null_count = top_genres.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"WARNING: {null_count} null values in {col_name} for top_genres")
    
    # === 8. Write Outputs to S3 ===
    print("Writing outputs to S3...")
    
    # Write with coalesce to reduce number of output files
    print("Writing Genre KPIs...")
    genre_kpi.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/{kpi_output_path}")
    
    print("Writing Top Songs...")
    top_songs.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/{top_songs_output_path}")
    
    print("Writing Top Genres...")
    top_genres.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{bucket}/{top_genres_output_path}")
    
    print("Transform job completed successfully!")
    
    # === 9. Final validation ===
    print("Final output validation:")
    
    # Read back and verify
    genre_kpi_output = spark.read.option("header", True).csv(f"s3://{bucket}/{kpi_output_path}")
    top_songs_output = spark.read.option("header", True).csv(f"s3://{bucket}/{top_songs_output_path}")
    top_genres_output = spark.read.option("header", True).csv(f"s3://{bucket}/{top_genres_output_path}")
    
    print(f"Final output counts - Genre KPIs: {genre_kpi_output.count()}, Top Songs: {top_songs_output.count()}, Top Genres: {top_genres_output.count()}")
    
    # Show final schemas
    print("Final Top Genres Schema:")
    top_genres_output.printSchema()
    print("Sample Top Genres Output:")
    top_genres_output.show(10)

except Exception as e:
    print(f"Transform job failed: {str(e)}")
    import traceback
    print(f"Full traceback: {traceback.format_exc()}")
    raise e

finally:
    spark.stop()