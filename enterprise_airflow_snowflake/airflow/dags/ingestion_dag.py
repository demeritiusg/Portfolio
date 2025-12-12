from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
}

SPARK_SCRIPT = "/enterprise_airflow_snowflake/spark_jobs/ingestion/s3_customer_ingest.py"

with DAG(
    dag_id="customer_data_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catcthup=False,
    tags=["pyspark","ingestion"],
) as dag:
    
    extract_s3_metadata = BashOperator(
        task_id="extract_s3_metadata",
        bash_command=
            "aws s3 ls s3://raw_bucket/customer_data/ --recursive ",
    )
        
    spark_ingestiont_task = BashOperator(
        task_id="run_spark_ingestion",
        bash_command=f"spark-submit -- master local[2] {SPARK_SCRIPT}",
    )
    
    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="python /scripts/load_to_snowflake.py",
    )
    
    extract_s3_metadata >> spark_ingestiont_task >> load_to_snowflake