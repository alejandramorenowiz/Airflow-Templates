import sys
import os.path
import io
from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
import airflow.utils.dates

# Stopper how to upload custom modules
#sys.path.append("/opt/airflow/dags/custom_modules")
from custom_modules.postgres_to_s3 import PostgresToS3Operator

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('s3_dag_insert_userpurchases_PostgrestoS3', 
        default_args = default_args,
        description='Insert Data from Postgres To S3',
        schedule_interval='@once',        
        catchup=False)

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
