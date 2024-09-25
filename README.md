# Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform

# Project Overview
## Introduction
This project built a near-real-time data pipeline to ingest, process, and analyze log data, laying the foundation for future analytics and insights on a recruitment platform.

**Technologies used**: Kafka, Cassandra, PySpark, MySQL, Airflow, Python, Docker

## Architecture
![project-pipeline](https://github.com/hien2706/Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform/blob/main/images/pipeline-architecture.jpg)

Real-time log data was ingested via Kafka, stored in Cassandra, and transformed/loaded into MySQL using scheduled PySpark ETL jobs orchestrated by Airflow. Change Data Capture ensured data consistency. The entire pipeline was containerized with Docker for easy deployment and scalability.

# ETL Pipeline
## Ingestion
Raw log data is continuously captured and streamed into Kafka topics, providing a highly scalable and fault-tolerant ingestion mechanism. 
## ETL
PySpark ETL jobs extract raw log data from Cassandra, perform transformations, and load the processed data into MySQL for efficient querying and reporting.

Transformation logic: 
- calculate click rate , conversion rate, disqualified applications, qualified applications for each hour, job_id, publisher_id, campaign_id, group_id
- Caculating bit_set and spend_hour for clicks

Before transformation:

| Column          | Datatype |
|-----------------|----------|
| create_time     | text     |
| bid             | int      |
| bn              | text     |
| campaign_id     | int      |
| cd              | int      |
| custom_track    | text     |
| de              | text     |
| dl              | text     |
| dt              | text     |
| ed              | text     |
| ev              | int      |
| group_id        | int      |
| id              | text     |
| job_id          | int      |
| md              | text     |
| publisher_id    | int      |
| rl              | text     |
| sr              | text     |
| ts              | text     |
| tz              | int      |
| ua              | text     |
| uid             | text     |
| utm_campaign    | text     |
| utm_content     | text     |
| utm_medium      | text     |
| utm_source      | text     |
| utm_term        | text     |
| v               | int      |
| vp              | text     |

After Transformation:

| Column                  | Datatype |
|--------------------------|----------|
| id                       | int      |
| job_id                   | int      |
| dates                    | text     |
| hours                    | int      |
| disqualified_application | int      |
| qualified_application    | int      |
| conversion               | int      |
| company_id               | int      |
| group_id                 | int      |
| campaign_id              | int      |
| publisher_id             | int      |
| bid_set                  | double   |
| clicks                   | int      |
| impressions              | text     |
| spend_hour               | double   |
| sources                  | text     |

Change Data Capture (CDC) mechanisms were implemented to continuously monitor Cassandra for updates and efficiently propagate those changes to MySQL, ensuring data consistency between the two databases.

## Visualization
 A Grafana dashboard connected to MySQL provides near real-time visualizations and monitoring
![grafana-dashboard](https://github.com/hien2706/Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform/blob/main/images/grafana-dashboard.jpg)
