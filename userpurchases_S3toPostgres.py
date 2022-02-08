from datetime import timedelta
from airflow import DAG
import airflow.utils.dates

import sys

#sys.path.append("/opt/airflow/dags/repo/custom_modules")
from custom_modules.s3_to_postgres import S3ToPostgresTransfer

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'schedule_interval'=@once,        
    'start_date'=datetime(2022, 1, 1),
     catchup=False
}

dag = DAG('dag_insert_userpurchases_s3toPostgres', 
        default_args = default_args,
        description='Insert Data from CSV in S3 To Postgres')

dag = DAG('dag_insert_userpurchases_s3toPostgres', 
          description='Insert Data from CSV To Postgres',
          schedule_interval='@once',        
          start_date=datetime(2022, 1, 1),
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
