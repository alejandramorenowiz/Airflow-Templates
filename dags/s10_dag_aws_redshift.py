from datetime import datetime
from airflow import DAG
import json

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

query1 = ["""
            CREATE TABLE IF NOT EXISTS fma_schema.dim_os (
                id_dim_os varchar(256) NOT NULL,
                os varchar(256),
                CONSTRAINT os_pkey PRIMARY KEY (id_dim_os)
            );
          """,
          """
            CREATE TABLE IF NOT EXISTS fma_schema.dim_browser (
                id_dim_browser varchar(256) NOT NULL,
                browser varchar(256),
                CONSTRAINT browser_pkey PRIMARY KEY (id_dim_browser)
            );
           """,
          """
            CREATE TABLE IF NOT EXISTS fma_schema.dim_devices (
                id_dim_device varchar(256) NOT NULL,
                device varchar(256),
                CONSTRAINT device_pkey PRIMARY KEY (id_dim_devices)
            );
           """,
           """
            CREATE TABLE IF NOT EXISTS fma_schema.dim_location (
                id_dim_location varchar(256) NOT NULL,
                location varchar(256),
                CONSTRAINT location_pkey PRIMARY KEY (id_dim_location)
            );
          """,
          """
            CREATE TABLE IF NOT EXISTS fma_schema.dim_date (
                id_dim_date varchar(256) NOT NULL,
                log_date varchar(256),
                day varchar(256),
                month varchar(256),
                year varchar(256),
                season varchar(256),
                CONSTRAINT date_pkey PRIMARY KEY (id_dim_date)
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
