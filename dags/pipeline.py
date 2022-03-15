import sys
from datetime import datetime
import os.path
import pandas as pd
import io
import warnings

sys.path.append("/opt/airflow/dags/repo/custom_modules")
from s3_to_postgres import S3ToPostgresTransfer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


import boto3
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import  EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

s3_key = "..."
s3_bucket = "..."

BUCKET_NAME = "customerbucketam"
s3_data = "..."
s3_script = "...."
s3_clean = "..."
logs_location = "..."

class user_purchase():

    @apply_defaults
    def __init__(
        self,
        invoice_number,
        stock_code,
        detail,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country
    ):
        super(user_purchase, self).__init__()
        self.invoice_number = invoice_number
        self.stock_code = stock_code
        self.detail = detail
        self.quantity = quantity
        self.invoice_date = invoice_date
        self.unit_price = unit_price
        self.customer_id = customer_id
        self.country = country

class postgresql_to_s3(BaseOperator):

    template_fields = ()
    template_ext = ()

    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        s3_bucket,
        s3_key,
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        verify=None,
        wildcard_match=False,
        copy_options=tuple(),
        autocommit=False,
        parameters=None,
        *args,
        **kwargs
    ):
        super(postgresql_to_s3, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id = aws_conn_postgres_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):

        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_client = self.s3.get_conn()
        self.current_table = self.schema + "." + self.table
        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)
        fieldnames = ['invoice_number', 'stock_code', 'detail', 'quantity', 'invoice_date', 'unit_price', 'customer_id', 'country']

        queries_list = ["select * from bootcampdb.user_purchases where invoice_date BETWEEN '2010-12-01 08:26:00'::timestamp and '2011-04-01 00:00:00'::timestamp", "select * from bootcampdb.user_purchases where invoice_date BETWEEN '2011-04-01 00:00:00'::timestamp and '2011-07-01 00:00:00'::timestamp", "select * from bootcampdb.user_purchases where invoice_date BETWEEN '2011-07-01 00:00:00'::timestamp and '2011-10-01 00:00:00'::timestamp", "select * from bootcampdb.user_purchases where invoice_date BETWEEN '2011-10-01 00:00:00'::timestamp and '2011-11-01 00:00:00'::timestamp", "select * from bootcampdb.user_purchases where invoice_date BETWEEN '2011-11-01 00:00:00'::timestamp and '2011-12-09 12:50:00'::timestamp"]
        outfileStr=""
        f = StringIO(outfileStr)
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for query in queries_list:
            
            self.request = (query)        
            self.log.info(self.request)
            self.connection = self.pg_hook.get_conn()
            self.cursor = self.connection.cursor()
            self.cursor.execute(self.request)
            self.sources = self.cursor.fetchall()
            self.log.info(self.sources)

            for source in self.sources:
                obj = user_purchase(
                    invoice_number=source[0],
                    stock_code=source[1],
                    detail=source[2],
                    quantity=source[3],
                    invoice_date=source[4],
                    unit_price=source[5],
                    customer_id=source[6],
                    country=source[7]
                )
                w.writerow(vars(obj))
        s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_key, Body=f.getvalue())

#MOVIES_ABNALYTICS
SPARK_STEPS_movie = [ # Note the params values are supplied to the operator
   
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://spark-jobscripts/movie_review_logic.py",
            ],
        },
    }  
]


JOB_FLOW_OVERRIDES = {
    "Name": "Transformation stage",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri" : f"s3://{BUCKET_NAME}/{logs_location}",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    'Steps': SPARK_STEPS_movie,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

#LOGS_ABNALYTICS
SPARK_STEPS_logs = [ # Note the params values are supplied to the operator
   
    {
        "Name": "Classify log reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "com.databricks:spark-xml_2.12:0.14.0",
                "s3://spark-jobscripts/log_review_logic.py",
            ],
        },
    }  
]


JOB_FLOW_OVERRIDES = {
    "Name": "Logs review classifier",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri" : f"s3://{BUCKET_NAME}/{logs_location}",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    'Steps': SPARK_STEPS_logs,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

#LOGS BROWSER
SPARK_STEPS_logsbrowser = [ # Note the params values are supplied to the operator
   
    {
        "Name": "Classify log reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "com.databricks:spark-xml_2.12:0.14.0",
                "s3://spark-jobscripts/log_review_logic_b.py",
            ],
        },
    }  
]


JOB_FLOW_OVERRIDES = {
    "Name": "Logs review classifier",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri" : f"s3://{BUCKET_NAME}/{logs_location}",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    'Steps': SPARK_STEPS_logsbrowser,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

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
                        invoiceNo varchar,
                        stockCode varchar,
                        description varchar,
                        quantity varchar,
                        invoiceDate varchar,
                        unitPrice varchar,                           
                        customerID varchar,
                        country varchar
                      )
                      stored as PARQUET
                      LOCATION 's3://staging-layer20220307050201862200000005/user_purchase_.parquet/';
                      """]

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

dag = DAG('complete_pipeline', 
        default_args = default_args,     
        description='Insert Data from CSV in S3 To Postgres',
        schedule_interval='@once')


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

moview_review_raw_transfer = S3CopyObjectOperator(
                            task_id = 'dag_movie_review_transfer',
                            source_bucket_key='movie_review.csv',
                            dest_bucket_key='movie_review.csv',
                            aws_conn_id='aws_default',
                            source_bucket_name='customerbucketam',
                            dest_bucket_name='raw-layer20220307050201862300000006', 
                            dag = dag
)

log_review_raw_transfer = S3CopyObjectOperator(
                            task_id = 'dag_log_review_transfer',
                            source_bucket_key='log_reviews.csv',
                            dest_bucket_key='log_reviews.csv',
                            aws_conn_id='aws_default',
                            source_bucket_name='customerbucketam',
                            dest_bucket_name='raw-layer20220307050201862300000006', 
                            dag = dag
)

postgres_to_s3 = postgresql_to_s3(
        task_id="dag_postgres_to_s3",
        schema="dbname", 
        table="user_purchase",
        s3_bucket="staging-layer20220307050201862200000005",
        s3_key="user_purchase_data_from_postgres.csv",
        aws_conn_postgres_id="postgres_default",
        aws_conn_id="aws_default",
        dag = dag
        )       
 
# Create an EMR cluster - MOVIES
create_emr_cluster_movie = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster1",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Check job flow - MOVIES
job_sensor_movie = EmrJobFlowSensor(task_id='check_job_flow1',
 job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
 dag = dag)

# Create an EMR cluster - LOGS
create_emr_cluster_logs = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster2",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Check job flow - LOGS
job_sensor_logs = EmrJobFlowSensor(task_id='check_job_flow2',
 job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
 dag = dag)


# Create an EMR cluster - LOGSBROWSER
create_emr_cluster_logsbrowser = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster3",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Check job flow
job_sensor_logsbrowser = EmrJobFlowSensor(task_id='check_job_flow3',
 job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
 dag = dag)


setup_task_create_tables = PostgresOperator(
        postgres_conn_id='redshift_default',
        task_id='setup_task_create_table',
        sql= _query,
        autocommit = True,
        dag = dag
        )


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



s3_to_postgres_operator >> postgres_to_s3

[moview_review_raw_transfer, log_review_raw_transfer ] >>  create_emr_cluster_movie >> job_sensor_movie >> create_emr_cluster_logs >> job_sensor_logs >> create_emr_cluster_logsbrowser >> job_sensor_logsbrowser

setup_task_create_tables >>  setup_dim_tables >> dim_tables
