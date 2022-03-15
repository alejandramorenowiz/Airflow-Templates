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
