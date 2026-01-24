# Event-Driven Athena–Redshift Data Sync Pipeline

Project Description

This project implements an event-driven data pipeline that ingests data into Athena Iceberg tables, exposes the same data through Redshift external tables, and finally loads it into Redshift local schema tables for analytics consumption.

The pipeline is fully automated using AWS Glue, Airflow, Lambda, S3 event notifications, and SES, ensuring near real-time processing, downstream notifications, and reliable orchestration.

## Overview
This project is an event-driven data processing pipeline that automates data ingestion, transformation, and availability across Athena Iceberg and Amazon Redshift.

The pipeline listens for new data arrivals in S3, triggers processing through AWS Glue and Airflow, and ensures data is available in both Athena and Redshift for downstream analytics teams.

---

## Architecture Components

| Component | Purpose |
|--------|--------|
| Amazon S3 | Source and storage layer for raw and processed data |
| S3 Event Notification | Triggers Lambda on new file arrival |
| AWS Lambda | Triggers Airflow DAG execution |
| Apache Airflow | Orchestrates Glue job execution |
| AWS Glue | Runs Spark jobs for data processing |
| Athena (Iceberg) | Stores processed data in Iceberg format |
| Redshift External Tables | Provides access to Iceberg data |
| Amazon Redshift | Stores finalized data in local schema tables |
| Amazon SES | Sends data availability notifications |

---

## High-Level Flow

1. New data arrives in the source S3 bucket  
2. S3 event notification triggers a Lambda function  
3. Lambda triggers an Airflow DAG  
4. Airflow starts an AWS Glue Spark job  
5. Glue processes data and writes to Athena Iceberg tables  
6. Data becomes available via Redshift external tables  
7. Data is copied into Redshift local schema tables  
8. SES sends email notifications to downstream users  

---

## Data Processing Steps

1. **Ingest Data**  
   - Glue Spark job reads raw data from S3

2. **Transform & Store**  
   - Data is transformed and written to Athena Iceberg tables

3. **Sync to Redshift**  
   - Data is exposed via Redshift external tables and copied to local tables

---

## Orchestration Details

- **Trigger Type:** Event-driven  
- **Scheduler:** Apache Airflow  
- **Processing Engine:** AWS Glue (Spark)  
- **Notification:** Amazon SES  

---

## Notifications

- SES emails are sent from within the Spark job
- Downstream users are notified once data is successfully available

---

## Technologies Used

- AWS Glue (Spark)
- Amazon S3
- Apache Airflow
- AWS Lambda
- Amazon Athena (Iceberg)
- Amazon Redshift
- Amazon SES

---

## Key Benefits

- Fully automated and event-driven
- Scalable Spark-based processing
- Unified data availability across Athena and Redshift
- Reliable orchestration with monitoring
- Proactive downstream notifications

---

---

## Flow Diagram

<img width="1829" height="944" alt="image" src="https://github.com/user-attachments/assets/6093c2d4-0132-414a-be9e-016b06ea4535" />

---

## Future Enhancements

- Add data quality validations
- Implement retry and backfill logic
- Improve monitoring and alerting
- Add schema evolution handling
