import sys
from datetime import datetime
import os.path
import pandas as pd
import io
import warnings
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
sys.path.append("/opt/airflow/dags/repo/operators")
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
sys.path.append("/opt/airflow/dags/repo/helpers")
from helpers import SqlQueries

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('aws_redshift_dag',
          default_args=default_args,
          description='Extract data from S3 and transform to Redshift',
          schedule_interval='@once',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
schema_created = DummyOperator(task_id='Schema_created', dag=dag)
run_quality_checks = DummyOperator(task_id='Run_data_quality_checks', dag=dag)

create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_create
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag, table='songs',
    select_sql=SqlQueries.songs_table_insert,
    mode='truncate'
)


start_operator >> create_songs_table
create_songs_table >> schema_created
schema_created >> load_song_dimension_table
load_song_dimension_table >> run_quality_checks
