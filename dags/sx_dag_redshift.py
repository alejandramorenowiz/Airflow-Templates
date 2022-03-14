import airflow.utils.dates
from airflow import DAG
import json

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
                
                      """
                      create external table fma_schema.movie_reviews(
                        cid varchar,
                        positive_review integer,
                        id_review varchar
                      )
                      stored as PARQUET
                      LOCATION 's3://customerbucketam/classification_result.parquet/';
                      """]


s3_key = "..."
s3_bucket = "..."

default_args = {
    'owner': 'ale',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('sx_dag_redshift', default_args = default_args, schedule_interval = '@daily')

setup_task_create_tables = RedshiftSQLOperator(
        redshift_conn_id="redshift_default"
        task_id='setup_task_create_table',
        sql= _query,
        autocommit = True,
        dag = dag
        )
