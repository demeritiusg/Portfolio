from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueOperator
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "retries": 2,
}

with DAG(
    dag_id="glue_sales_transoform",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_intervial="@daily",
    catchup=False,
    tags=["glue", "aws", "etl"],
) as dag:
    
    glue_job = AwsGlueOperator(
        task_id="run_sales_glue_transform",
        job_name="sales_transofm_job",
        script_location="s3://my-glue-scripts/sales_transform.py",
        aws_conn_id="aws_default",
        region_name="us-west-2",
        num_workers=5,
        worker_type="G.1X",
    )
    
    glue_job