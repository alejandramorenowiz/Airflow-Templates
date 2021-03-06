import sys
from datetime import datetime
import os.path
import pandas as pd
import io
from airflow import DAG
#from custom_modules.operator_s3_to_postgres import S3ToPostgresTransfer
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException


from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


import boto3
from botocore.exceptions import ClientError



def print_welcome():
    return 'Welcome from custom operator - Airflow DAG!'

class S3ToPostgresTransfer(BaseOperator):
    """S3ToPostgresTransfer: custom operator created to move small csv files of data
                             to a postgresDB, it was created for DEMO.
       Author: Juan Escobar.
       Creation Date: 20/09/2022.
    Attributes:
    """

    template_fields = ()

    template_ext = ()

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            aws_conn_postgres_id='postgres_default',
            aws_conn_id='aws_default',
            verify=None,
            wildcard_match=False,
            copy_options=tuple(),
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
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

        self.log.info('Into the custom operator S3ToPostgresTransfer')

        # Create an instances to connect S3 and Postgres DB.
        self.log.info(self.aws_conn_postgres_id)

        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)
        self.log.info("Init PostgresHook..")
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info("Downloading S3 file")
        self.log.info(self.s3_key + ', ' + self.s3_bucket)

        # Validate if the file source exist or not in the bucket.
        try:
            if self.wildcard_match:
                if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                    raise AirflowException("No key matches {0}".format(self.s3_key))
                s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
            else:
                if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                    raise AirflowException(
                        "The key {0} does not exists".format(self.s3_key))
        except ClientError as e:
            print(e.response)
            print(e.response['Error'])
            print(e.response['Error']['Code'])
            print("Oops!", sys.exc_info()[0], "occurred.")
            raise e

        s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        # Read and decode the file into a list of strings.
        list_srt_content = s3_key_object.get()['Body'].read().decode(encoding="utf-8", errors="ignore")

        # schema definition for data types of the source.
        schema = {
            'invoice_number': 'string',
            'stock_code': 'string',
            'detail': 'string',
            'quantity': 'int',
            'unit_price': 'float64',
            'customer_id': 'string',
            'country': 'string',
        }
        custom_date_parser = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M")

        # read a csv file with the properties required.
        df_userpurchases = pd.read_csv(io.StringIO(list_srt_content),
                                  header=0,
                                  delimiter=",",
                                  quotechar='"',
                                  low_memory=False,
                                  parse_dates=["InvoiceDate"],
                                  date_parser=custom_date_parser,
                                  dtype=schema
                                  )
        self.log.info(df_userpurchases)
        self.log.info(df_userpurchases.info())

        # formatting and converting the dataframe object in list to prepare the income of the next steps.
        df_userpurchases = df_userpurchases.replace(r"[\"]", r"'")
        list_df_userpurchases = df_userpurchases.values.tolist()
        list_df_userpurchases= [tuple(x) for x in list_df_userpurchases]
        self.log.info(list_df_userpurchases)

        # Read the file with the DDL SQL to create the table purchase in postgres DB.
        nombre_de_archivo = "bootcampdb.user_purchases.sql"

        ruta_archivo = os.path.sep + nombre_de_archivo
        self.log.info(ruta_archivo)
        # proposito_del_archivo = "r"  # r es de Lectura
        # codificaci??n = "UTF-8"  # Tabla de Caracteres,
        # ISO-8859-1 codificaci??n preferidad por
        # Microsoft, en Linux es UTF-8

        # with open(ruta_archivo, proposito_del_archivo, encoding=codificaci??n) as manipulador_de_archivo:
        #
        #     # Read dile with the DDL CREATE TABLE
        #     SQL_COMMAND_CREATE_TBL = manipulador_de_archivo.read()
        #     manipulador_de_archivo.close()
        #
        #     # Display the content
        #     self.log.info(SQL_COMMAND_CREATE_TBL)
        SQL_COMMAND_CREATE_TBL = """
        DROP TABLE IF EXISTS bootcampdb.user_purchases;
        
        DROP SCHEMA IF EXISTS bootcampdb;
        
        CREATE SCHEMA IF NOT EXISTS bootcampdb;
        CREATE TABLE IF NOT EXISTS bootcampdb.user_purchases (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity BIGINT,
            invoice_date timestamp,
            unit_price NUMERIC(8,3),
            customer_id VARCHAR(20),
            country VARCHAR(20)
        );
        """
        # Display the content
        self.log.info(SQL_COMMAND_CREATE_TBL)

            # execute command to create table in postgres.
        self.pg_hook.run(SQL_COMMAND_CREATE_TBL)

        # set the columns to insert, in this case we ignore the id, because is autogenerate.
        list_target_fields = ['invoice_number',
                              'stock_code',
                              'detail',
                              'quantity',
                              'invoice_date',
                              'unit_price',
                              'customer_id',
                              'country']

        self.current_table = self.schema + '.' + self.table
        self.pg_hook.insert_rows(self.current_table,
                                 list_df_userpurchases,
                                 target_fields=list_target_fields,
                                 commit_every=1000,
                                 replace=False)

        # Query and print the values of the table purchase in the console.
        self.request = 'SELECT * FROM ' + self.current_table
        self.log.info(self.request)
        self.connection = self.pg_hook.get_conn()
        self.cursor = self.connection.cursor()
        self.cursor.execute(self.request)
        self.sources = self.cursor.fetchall()
        self.log.info(self.sources)

        for source in self.sources:
            self.log.info("invoice_number: {0} - \
                           stock_code: {1} - \
                           detail: {2} - \
                           quantity: {3} - \
                           invoice_date: {4} - \
                           unit_price: {5} - \
                           customer_id: {6} - \
                           country: {7} ".format(source[0], source[1], source[2], source[3], source[4], source[5],
                                                 source[6],
                                                 source[7]))


dag = DAG('dag_insert_data', 
          description='Insert Data from CSV To Postgres',
          schedule_interval='@once',        
          start_date=datetime(2022, 1, 1),
          catchup=False)

welcome_operator = PythonOperator(task_id='welcome_task', 
                                  python_callable=print_welcome, 
                                  dag=dag)

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

welcome_operator >> s3_to_postgres_operator