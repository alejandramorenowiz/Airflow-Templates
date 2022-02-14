from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string


def ext_from_xml(xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

def ext_schema_of_xml_df(df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test_spark")\
               .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")\
               .master("local[2]").getOrCreate()

    # Read file
    df = spark.read.option("header", "true").csv("s3://customerbucketam/log_reviews.csv")

    #Process file
    logSchema = ext_schema_of_xml_df(df.select("log"))
    df = df.withColumn("parsed", ext_from_xml(df.log, logSchema))
    df = df.drop("log")
    df_processed = df_raw.select("id_review", "parsed.log.*")

    df_processed.write.mode("overwrite").parquet("s3://customerbucketam/log_reviews_clean.csv")
