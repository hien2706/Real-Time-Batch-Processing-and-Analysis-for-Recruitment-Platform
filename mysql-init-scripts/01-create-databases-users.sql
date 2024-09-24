-- Create database for Airflow
CREATE DATABASE IF NOT EXISTS airflow;

-- Create user for Airflow
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';


FLUSH PRIVILEGES;
