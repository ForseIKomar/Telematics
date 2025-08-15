from tokenize import group
from  db_utils.postgres_utils_v2 import fetch_task_parameters, insert_multiple_task_history, call_procedure_and_return
import uuid
from airflow.decorators import task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.hooks.S3_hook import S3Hook
from tempfile import NamedTemporaryFile
from os import stat
from airflow import DAG
import json
import traceback
from datetime import datetime, timedelta, date
from io import BytesIO
import pandas as pd
import boto3
     
     
def boto3_client_from_conn(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    client = boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=extra.get("endpoint_url"),
        region_name=extra.get("region_name", extra.get("aws_default_region", "us-east-1")),
    )
    return client
        
@task
def upload_chunk_to_s3(job_parameters, ti=None):
    """
    Читает лимит/оффсет из Postgres, конвертирует в Parquet (в памяти) и кладёт в S3.
    Ключ: {SOURCE_PREFIX_BASE}/{day}/packet_raw_data_{day}_offset_{offset:08d}.snappy.parquet
    """
    
    if len (job_parameters) == 0:
        return
    
    day = job_parameters["date"]
    limit = int(job_parameters["limit"])
    offset = int(job_parameters["offset"])
    rows_counter = int(job_parameters["rows_counter"])

    # Postgres -> pandas
    
    hook = PostgresHook(postgres_conn_id=f"local-{job_parameters['donor_database']}")
    
    sql = f'''
        SELECT device_id, device_protocol_type, raw_data, "timestamp"
        FROM "packet_raw_data_{day}"
        ORDER BY device_id, "timestamp"
        LIMIT {limit} OFFSET {offset}
    '''
    # Используем connection из hook, чтобы не плодить engine
    with hook.get_conn() as conn:
        df = pd.read_sql_query(sql, con=conn)

    if df.empty:
        ti.xcom_push(key='tasks_status_log', value={'task_status': [{'task_status': 204}],
                                                    'map_index': ti.map_index, 
                                                    'task_parameters' : job_parameters})
        return # Status = 204

    # Приводим типы (как в твоём коде)
    df["device_id"] = df["device_id"].astype(str)
    df["raw_data"] = df["raw_data"].astype(str)
    df["timestamp"] = df["timestamp"].astype(str)

    # In-memory parquet
    buf = BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)

    aws_conn = boto3_client_from_conn(f"local-{job_parameters['recipient_s3']}")
    bucket = job_parameters["s3"]["bucket"]
    prefix = job_parameters["s3"]["raw_data_prefix"]
    key = f"{prefix}/{day}/packet_raw_data_{day}_offset_{offset:08d}.snappy.parquet"

    s3 = boto3_client_from_conn(aws_conn)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    task_status = 201 if limit + offset < rows_counter else 200
    ti.xcom_push(key='tasks_status_log', value={'task_status': [{'task_status': task_status}],
                                                'map_index': ti.map_index, 
                                                'task_parameters' : job_parameters})

@task
def task_parameters_from_xcom(ti=None): 
    job_parameters_array = ti.xcom_pull(task_ids='fetch_task_parameters', key='task_parameters')

    if job_parameters_array is None:
        job_parameters_array = []
    
    return job_parameters_array

@task_group(group_id="task_factory")
def task_factory():
    upload_chunk_to_s3.expand(job_parameters=task_parameters_from_xcom())

interval = "*/5 * * * *"
start_date = datetime(year=2025, day=15, month=8, hour=00, minute=0)

with DAG(
    dag_id="local_s3_merge_history",
    start_date=start_date,
    max_active_runs=1,
    schedule_interval=interval,
    tags=["local-postgres", "local-s3"],
    catchup=False,
    render_template_as_native_obj=True
    ) as main_dag:

    fetch_task_parameters = fetch_task_parameters('merge_history', postgres_conn_id="local-datalake-postgres")
    tf = task_factory()
    insert_multiple_task_history = insert_multiple_task_history(postgres_conn_id="local-datalake-postgres")

    (
        fetch_task_parameters 
        >> tf
        >> insert_multiple_task_history
    )
