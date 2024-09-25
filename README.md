# Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform

# Project Overview
## Introduction
This project established a robust and scalable data engineering pipeline for the continuous ingestion, processing, and storage of log data in near real-time, laying the foundation for future analytics and insights on a recruitment platform.

**Technologies used**: Kafka, Cassandra, PySpark, MySQL, Airflow, Python, Docker

## Architecture
![project-pipeline](https://github.com/hien2706/Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform/blob/main/images/pipeline-architecture.jpg)

Real-time log data was ingested via Kafka, stored in Cassandra, and transformed/loaded into MySQL using scheduled PySpark ETL jobs orchestrated by Airflow. Change Data Capture ensured data consistency. The entire pipeline was containerized with Docker for easy deployment and scalability.

# Project details
## Ingestion
Raw log data is ingested into Cassandra through Kafka using streaming
## ETL
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

CDC mechanisms were implemented to efficiently identify and propagate updates from Cassandra to MySQL

## Visualization

