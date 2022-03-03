import sys
from datetime import datetime
import os.path
import pandas as pd
import io
import warnings

sys.path.append("/opt/airflow/dags/repo/custom_modules")
from s3_to_postgres import S3ToPostgresTransfer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


import boto3
from botocore.exceptions import ClientError

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('s2_dag_insert_userpurchases_s3toPostgres', 
        default_args = default_args,     
        description='Insert Data from CSV in S3 To Postgres',
        schedule_interval='@once')


s3_to_postgres_operator = S3ToPostgresTransfer(
                            task_id = 'dag_s3_to_postgres',
                            schema =  'bootcampdb',
                            table= 'user_purchases',
                            s3_bucket = 'raw-layer20220303070810341300000006',
                            s3_key =  'user_purchase.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                            dag = dag
)

s3_to_postgres_operator

