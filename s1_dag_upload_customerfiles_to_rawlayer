import sys
import os.path
import io
from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
import airflow.utils.dates

from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator


default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}


dag = DAG('s1_dag_upload_customerfiles_to_rawlayer', 
        default_args = default_args,
        description='Upload customer files (moview_review.csv & log_review.csv) to Raw layer s3 bucket',
        schedule_interval='@once',        
        catchup=False)


moview_review_raw_transfer = S3CopyObjectOperator(
                            task_id = 'dag_movie_review_transfer',
                            source_bucket_key='movie_review.csv',
                            dest_bucket_key='movie_review.csv',
                            aws_conn_id='aws_default',
                            source_bucket_name='customerbucketam',
                            dest_bucket_name='staging-layer20220217062437544100000006', 
                            dag = dag
)

log_review_raw_transfer = S3CopyObjectOperator(
                            task_id = 'dag_log_review_transfer',
                            source_bucket_key='log_reviews.csv',
                            dest_bucket_key='log_reviews.csv',
                            aws_conn_id='aws_default',
                            source_bucket_name='customerbucketam',
                            dest_bucket_name='staging-layer20220217062437544100000006', 
                            dag = dag

moview_review_raw_transfer > log_review_raw_transfer