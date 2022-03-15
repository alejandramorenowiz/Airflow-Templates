from datetime import datetime
from airflow import DAG
import json

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

query1 = ["""
            CREATE TABLE IF NOT EXISTS public.songs (
                id_os varchar(256) NOT NULL,
                os varchar(256),
                CONSTRAINT os_pkey PRIMARY KEY (id_os)
            );
           """]



default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('s10_dim_DDL_redshift', 
        default_args = default_args,     
        description='Insert user_purchases, moview_review and log_review tables',
        schedule_interval='@once')


setup_dim_tables = PostgresOperator(
        postgres_conn_id='redshift_default',
        task_id='setup_dim_table',
        sql= query1,
        autocommit = True,
        dag = dag
        )
