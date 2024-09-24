from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A DAG to run Spark job',
    schedule_interval=timedelta(days=1),
)

run_spark_job = DockerOperator(
    task_id='run_spark_job',
    image='pyspark',  # This should match the service name in your docker-compose
    command='spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,mysql:mysql-connector-java:8.0.32 /app/spark_job/ETL_pipeline.py',
    network_mode='etl_network',
    docker_url='unix://var/run/docker.sock',
    dag=dag,
)

run_spark_job