from datetime import datetime
from airflow import DAG
import json

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

_query = ["""
                      create external schema if not exists fma_schema
                      from data catalog
                      database 'apprdb'
                      iam_role 'arn:aws:iam::306718468668:role/service-role/AmazonRedshift-CommandsAccessRole-20220309T025558'
                      create external database if not exists;""",

                      "drop table if exists fma_schema.log_reviews;",
                      "drop table if exists fma_schema.movie_reviews;",
                      "drop table if exists fma_schema.user_purchase;",
                      "drop table if exists fma_schema.dim_os;",
                
                      """
                      create external table fma_schema.log_reviews(
                        id_review varchar,
                        device varchar,
                        ipAddress varchar,
                        location varchar,
                        logDate varchar,    
                        os varchar,           
                        phoneNumber varchar,
                        browser varchar
                      )
                      stored as PARQUET
                      LOCATION 's3://staging-layer20220307050201862200000005/log_reviews_clean_b.parquet';
                      """,
          
                      """
                      create external table fma_schema.movie_reviews(
                        cid varchar,
                        positive_review integer,
                        id_review varchar
                      )
                      stored as PARQUET
                      LOCATION 's3://staging-layer20220307050201862200000005/movie_classification_result.parquet/';
                      """,
         
                      """
                      create external table fma_schema.user_purchase(
                        invoice_number varchar,
                        stock_code varchar,
                        detail varchar,
                        quantity int,
                        invoice_date timestamp,
                        unit_price numeric,                           
                        customer_id int,
                        country varchar
                      )
                      stored as PARQUET
                      LOCATION 's3://staging-layer20220307050201862200000005/user_purchase_data_from_postgres.parquet/';
                      #row format delimited
                      #fields terminated by ','
                      #stored as textfile
                      #location 's3://staging-layer20220307050201862200000005/user_purchase_data_from_postgres.json/'
                      #table properties ('skip.header.line.count'='1');
                      """]


s3_key = "..."
s3_bucket = "..."

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('s7_basic_DDL_redshift', 
        default_args = default_args,     
        description='Insert user_purchases, moview_review and log_review tables',
        schedule_interval='@once')


setup_task_create_tables = PostgresOperator(
        postgres_conn_id='redshift_default',
        task_id='setup_task_create_table',
        sql= _query,
        autocommit = True,
        dag = dag
        )
