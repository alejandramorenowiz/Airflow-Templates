import sys
import os.path
import io
from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
import airflow.utils.dates

# Stopper how to upload custom modules
#sys.path.append("/opt/airflow/dags/custom_modules")
from .s3_to_postgres import S3ToPostgresTransfer

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_userpurchases_s3toPostgres', 
        default_args = default_args,
        description='Insert Data from CSV in S3 To Postgres',
        schedule_interval='@once',        
        catchup=False)

s3_to_postgres_operator = S3ToPostgresTransfer(
                            task_id = 'dag_s3_to_postgres',
                            schema =  'bootcampdb',
                            table= 'user_purchases',
                            s3_bucket = 'customerbucketam',
                            s3_key =  'user_purchase.csv',
                            aws_conn_postgres_id = 'postgres_default',
                            aws_conn_id = 'aws_default',   
                            dag = dag
)

s3_to_postgres_operator
