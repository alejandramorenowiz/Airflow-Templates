from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from typing import List, Optional, Union
import pandas as pd

class PostgresToS3Operator(BaseOperator):

    def __init__(
        self,
        *,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        postgres_conn_id: str = 'postgres_default',
        aws_conn_id: str = 'aws_default',
        verify=None,
        wildcard_match=False,
        column_list: Optional[List[str]] = None,
        copy_options: Optional[List] = None,
        autocommit: bool = False,
        method: str = 'APPEND',
        upsert_keys: Optional[List[str]] = None,
        **kwargs):
        super(PostgresToS3Operator, self).__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.column_list = column_list
        self.copy_options = copy_options or []
        self.autocommit = autocommit
        self.method = method
        self.upsert_keys = upsert_keys

        # attributes that get their values during execution
        self.pg_hook = None
        self.s3 = None
        self.current_table = None

    def execute(self, context) -> None:
        self.log.info('Executing custom operator S3ToPostgresTransfer')

        # get staging layer unique bucket name in XCom
        #task_instance = context['task_instance']
        #value = task_instance.xcom_pull(task_ids="get_s3_bucket_names")
        #self.s3_bucket = value["staging"]
        #self.log.info("bucket name: {0}".format(value))

        # Function calls
        df = self.postgres_to_pandas(context)
        self.pandas_to_s3(df)

    def postgres_to_pandas(self, context):
        
        # Create an instances to connect Postgres DB and S3
        self.pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(" PostgresHook connected")
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        self.log.info(" AWS S3 connected")

        # Download Postgres table
        self.log.info("Downloading Postgres table: {0}".format(self.s3))
        self.current_table = self.schema + '.' + self.table
        request = "SELECT * FROM " + self.current_table
        
        connection = self.pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        source = cursor.fetchall()
        
        table_cols = "SELECT * FROM information_schema.columns " \
                       "WHERE table_schema = 'debootcamp' " \
                       "AND table_name = 'user_purchases';"
        cursor.execute(table_cols)
        cols = cursor.fetchall()
        cols = [k[3] for k in cols]
        self.log.info("columns: {0}".format(cols))

        # Fetch to dataframe
        df = pd.DataFrame(source, columns=cols)
        self.log.info("df: {0}".format(df))
        return df

    def pandas_to_s3(self, df):

        self.log.info("Loading dataframe to S3 {0}".format(df))
        self.s3.load_string(string_data=df.to_csv(index=False),
                            key="user_purchase_v.csv",
                            bucket_name=self.s3_bucket,
                            replace=True)