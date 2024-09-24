# Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform

# Project Overview
This project established a robust and scalable data engineering pipeline for the continuous ingestion, processing, and storage of log data in near real-time, laying the foundation for future analytics and insights on a recruitment platform.

Real-time log data was ingested via Kafka, stored in Cassandra, and transformed/loaded into MySQL using scheduled PySpark ETL jobs orchestrated by Airflow. Change Data Capture ensured data consistency. The entire pipeline was containerized with Docker for easy deployment and scalability.

Technologies used: Kafka, Cassandra, PySpark, MySQL, Airflow, Python, Docker

# Architecture

![project-pipeline][https://github.com/hien2706/Near-real-time-Log-Data-Processing-and-Analysis-for-Recruitment-Platform/blob/main/images/pipeline-architecture.jpg]
