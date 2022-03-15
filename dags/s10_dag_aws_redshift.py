from datetime import datetime
from airflow import DAG
import json

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

query1 = ["""
            drop table if exists apprdb.public.dim_os;
            drop table if exists apprdb.public.dim_browser;
            drop table if exists apprdb.public.dim_devices;
            drop table if exists apprdb.public.dim_location;
            drop table if exists apprdb.public.dim_date;          
         """,  
         """  
            CREATE TABLE IF NOT EXISTS public.dim_os (
                id_dim_os BIGINT identity(1, 1) NOT NULL,
                os varchar(256),
                primary key(id_dim_os)
            );
          """,
          """
            CREATE TABLE IF NOT EXISTS public.dim_browser (
                id_dim_browser BIGINT identity(1, 1) NOT NULL,
                browser varchar(256),
                primary key(id_dim_browser)
            );
           """,
          """
            CREATE TABLE IF NOT EXISTS public.dim_devices (
                id_dim_device BIGINT identity(1, 1) NOT NULL,
                device varchar(256),
                primary key(id_dim_device)
            );
           """,
           """
            CREATE TABLE IF NOT EXISTS public.dim_location (
                id_dim_location BIGINT identity(1, 1) NOT NULL,
                location varchar(256),
                primary key(id_dim_location)
            );
          """,
          """
            CREATE TABLE IF NOT EXISTS public.dim_date (
                id_dim_date BIGINT identity(1, 1) NOT NULL,
                log_date varchar(256),
                day varchar(256),
                month varchar(256),
                year varchar(256),
                season varchar(256),
                primary key(id_dim_date)
            );
           """]

query2 = ["""
           INSERT INTO apprdb.public.dim_os (os) (SELECT distinct os
           FROM fma_schema.log_reviews);
           """,
          """
           INSERT INTO apprdb.public.dim_browser (os) (SELECT distinct browser
           FROM fma_schema.log_reviews);
           """,
          """
           INSERT INTO apprdb.public.dim_devices (os) (SELECT distinct device
           FROM fma_schema.log_reviews);
           """,
          """
           INSERT INTO apprdb.public.dim_location (os) (SELECT distinct location
           FROM fma_schema.log_reviews);
           """,
         ]


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

dim_tables = PostgresOperator(
        postgres_conn_id='redshift_default',
        task_id='_dim_table',
        sql= query2,
        autocommit = True,
        dag = dag
        )

setup_dim_tables >> dim_tables
