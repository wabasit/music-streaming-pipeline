
# Music Data Processing Pipeline

An end-to-end ETL pipeline that processes song, user, and stream data using AWS Glue and Apache Airflow (via Amazon MWAA), performing data validation, transformation, and loading into DynamoDB, with automated alerts and S3 archival.


---

## Overview

This pipeline automates the extraction, validation, transformation, and loading (ETL) of music streaming data. It is orchestrated with Apache Airflow on Amazon MWAA, and leverages AWS Glue jobs for batch processing and validation.

---

## Architecture

```
S3 (raw data)
   │
   └──▶ S3KeySensor (Airflow)
         │
         └──▶ run_validation_job (Glue: validation checks)
                 │
                 └──▶ check_validation_status
                           ├──▶ Success → run_transform_job (Glue)
                           └──▶ Failure → notify_failure (SNS)
                                     │
                                     ↓
                               check_transform_status
                                     │
                                     ├──▶ Success → load_to_dynamodb
                                     │                ↓
                                     │          archive_streamed_data
                                     │                ↓
                                     │          notify_success
                                     └──▶ Failure → notify_failure
```

---

## Features

- **Schema & Null Validation** using PySpark in Glue jobs  
- **Bad Data Handling**: Invalid rows routed to separate S3 path  
- **Foreign Key Integrity** checks across songs, users, and streams  
- **Glue Transform Jobs** for cleaning & enrichment  
- **Airflow DAG** with branching logic and retries  
- **SNS Notification** on success/failure  
- **S3 Archival** of streamed files  

---

## Technologies

- **AWS S3**: Raw & Processed data storage  
- **AWS Glue**: Data processing and cleaning jobs  
- **AWS DynamoDB**: Stores KPI outputs  
- **Apache Airflow** (Amazon MWAA): Workflow orchestration  
- **Amazon SNS**: Success/Failure notifications  

---

## Setup Instructions

### Prerequisites

- AWS account with IAM roles set for:
  - Glue
  - S3
  - DynamoDB
  - SNS
- Amazon MWAA environment configured
- Airflow connection named `aws_default`

### Environment Variables (Airflow)

Set the following in Airflow Variables:

| Variable | Description |
|----------|-------------|
| `GLUE_JOB_NAME` | Name of the transformation Glue job |
| `DYNAMODB_GLUE_JOB_NAME` | Glue job for loading data to DynamoDB |
| `S3_BUCKET` | Main S3 bucket name |
| `S3_STREAMING_PREFIX` | S3 prefix for incoming streams |
| `SNS_TOPIC_ARN` | ARN of your SNS topic |

---

## DAG Flow Summary

### 1. Wait for Stream Files
Airflow watches S3 for new files under the defined streaming prefix.
Detect `.csv` files under `s3://<bucket>/raw_data/streams/`

### 2. Run Validation Glue Job
Checks schema, nulls, duplicates, ranges, and referential integrity

### 3. If Validation Passes
→ Runs transformation Glue job to clean & process data

### 4. Load to DynamoDB
→ Runs another Glue job to load results to 3 DynamoDB tables:
- `genre_kpis`
- `top_songs`
- `top_genres`

### 5. Archive Streamed Files
Moves used files to `archived/<date>/` folder in S3

### 6. Notifications
Sends success/failure message via Amazon SNS

---

## Glue Transform Functions

Each Glue transform uses a custom function that outputs:

```python
DynamicFrameCollection({
    "clean": clean_dyf,
    "bad": bad_dyf
}, glueContext)
```

- **`MyTransform_songs`**: Cleans song metadata  
- **`MyTransform_users`**: Validates user data and age range  
- **`MyTransform_streams`**: Ensures stream event completeness

---
## KPI Tables and Insights
1. genre_kpi – Genre-Level Engagement Summary

| Column            | Description                                      |
| ----------------- | ------------------------------------------------ |
| `genre`           | Music genre name (e.g., Pop, Rock)               |
| `date`            | Date of aggregation                              |
| `total_streams`   | Total stream count for this genre                |
| `unique_users`    | Number of distinct users who streamed this genre |
| `avg_listen_time` | Average listen duration (seconds)                |

### Insights:
- Spot genre trends across time (e.g., weekly rise in Afrobeat)
- Evaluate genre stickiness via average listen time
- Determine engagement patterns by unique users per genre


2. top_songs – Daily Ranked Songs by Genre

| Column         | Description                            |
| -------------- | -------------------------------------- |
| `date`         | Aggregation date                       |
| `genre`        | Song's genre                           |
| `track_id`     | Unique ID of the track                 |
| `title`        | Name of the song                       |
| `artist`       | Performing artist                      |
| `stream_count` | Number of streams recorded on that day |
| `rank`         | Rank based on descending stream count  |
| `date#genre`   | Composite key for DynamoDB             |

### Insights:
- Identify daily or weekly top-performing songs
- Track artist momentum over time
- Analyze which genres are contributing the most top tracks


3. top_genres – Ranked Genre Summary by Day

| Column          | Description                                   |
| --------------- | --------------------------------------------- |
| `date`          | Date of aggregation                           |
| `genre`         | Genre name                                    |
| `total_streams` | Total stream count for this genre on the date |
| `rank`          | Rank based on total daily stream count        |

### Insights:
- High-level genre trend monitoring
- Real-time performance of music categories
- Marketing insight for genre-focused campaigns
---

## 📂 Folder Structure

```
.
├── dags/
│   └── streaming_pipeline_dag.py
├── scripts/
│   ├── validate_job.py
│   ├── transform_job.py
│   └── load_to_dynamodb.py
├── data/
│   ├── songs/
│   ├── streams/
│   └── users/
├── processed_data/
│   ├── genre_kpis/
│   ├── top_genres/
│   └── top_songs/
├── bad_rows/
│   ├── songs_bad_rows.csv/
│   ├── streams_bad_rows.csv/
│   └── users_bad_rows.csv/
├── architecture.png
├── README.md
├── requirements.txt
├── README.md
```
---
