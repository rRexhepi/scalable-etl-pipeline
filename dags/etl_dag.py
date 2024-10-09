# dags/nyc_taxi_etl_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyc_taxi_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for NYC Taxi Trip Duration dataset',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 27),
    catchup=False,
) as dag:

    # Task 1: Extract - Upload raw data to MinIO
    extract = BashOperator(
        task_id='extract_data',
        bash_command="""
        aws s3 cp data/raw/train.csv s3://nyc-taxi/raw/train.csv
        aws s3 cp data/raw/test.csv s3://nyc-taxi/raw/test.csv
        aws s3 cp data/raw/sample_submission.csv s3://nyc-taxi/raw/sample_submission.csv
        """,
        env={
            'AWS_ACCESS_KEY_ID': 'minioadmin',
            'AWS_SECRET_ACCESS_KEY': 'minioadmin',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_ENDPOINT_URL': 'http://minio:9000'
        }
    )

    # Task 2: Transform - Run Spark job
    transform = SparkSubmitOperator(
        task_id='transform_data',
        application='/opt/airflow/scripts/transform.py',
        conn_id='spark_default',
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='2g',
        driver_memory='1g',
        conf={
            'spark.some.config.option': 'some-value'
        }
    )

    # Task 3: Load - Load data into PostgreSQL
    load = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_default',
        sql="""
        COPY nyc_taxi_trip_duration_train FROM '/opt/airflow/data_clean/cleaned_train.csv' DELIMITER ',' CSV HEADER;
        COPY nyc_taxi_trip_duration_test FROM '/opt/airflow/data_clean/cleaned_test.csv' DELIMITER ',' CSV HEADER;
        """,
    )

    # Define task dependencies
    extract >> transform >> load