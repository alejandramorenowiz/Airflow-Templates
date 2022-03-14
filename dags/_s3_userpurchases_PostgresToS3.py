import sys
from datetime import datetime
import os.path
import pandas as pd
import io
from io import StringIO
import warnings

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.task_group import TaskGroup

import boto3
import csv, re
 
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

        queries_list = ["select * from dbname.user_purchase where invoice_date BETWEEN '2010-12-01 08:26:00'::timestamp and '2011-04-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-04-01 00:00:00'::timestamp and '2011-07-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-07-01 00:00:00'::timestamp and '2011-10-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-10-01 00:00:00'::timestamp and '2011-11-01 00:00:00'::timestamp", "select * from dbname.user_purchase where invoice_date BETWEEN '2011-11-01 00:00:00'::timestamp and '2011-12-09 12:50:00'::timestamp"]
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


default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': 'None'
}

dag = DAG('_s3_dag_userpurchases_PostgresToS3', 
        default_args = default_args,
        description='Insert Data from Postgres to S3',
        schedule_interval='@once',        
        catchup=False)

 
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

postgres_to_s3
