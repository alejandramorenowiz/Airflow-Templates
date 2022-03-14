import sys
from datetime import datetime
import os.path
import pandas as pd
import io
import warnings

sys.path.append("/opt/airflow/dags/repo/custom_modules")
from postgres_to_s3 import PostgresToS3Operator

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import airflow.utils.dates

import boto3
from botocore.exceptions import ClientError


default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('_dag_insert_userpurchases_PostgrestoS3', 
        default_args = default_args,
        description='Insert Data from Postgres To S3',
        schedule_interval='@once')

postgres_to_S3_operator = PostgresToS3Operator(
                            task_id = 'dag_postgres_to_s3',
                            schema =  'bootcampdb',
                            table= 'user_purchases',
                            s3_bucket = 'customerbucketam',
                            s3_key =  'user_purchase_cop.csv',
                            postgres_conn_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                            dag = dag
)

postgres_to_S3_operator
