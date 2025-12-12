from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
}

JOB_FLOW_OVERRIDES = {
    "Name": "emr-sales-transform",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEP = [
    {
        "Name": "SalesTransformationStep",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://your-bucket/scripts/emr_sales_transform.py",
            ],
        },
    }
]

with DAG(
    dag_id="emr_sales_aggregation_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["emr", "pyspark", "aws"],
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster", job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    add_step = EmrAddStepsOperator(
        task_id="add_transform_step", job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}", steps=SPARK_STEP
    )

    watch_step = EmrStepSensor(
        task_id="watch_emr_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_transform_step')[0] }}",
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        trigger_rule="all_done",
    )

    create_cluster >> add_step >> watch_step >> terminate_cluster
