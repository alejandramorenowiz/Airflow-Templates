from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains, when


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test_spark")\
               .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")\
               .master("local[2]").getOrCreate()

    # Read file
    df = spark.read.option("header", "true").csv("s3://customerbucketam/movie_review.csv")

    # Tokenizer and StopwordsRemover
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    dftokenized = tokenizer.transform (df)

    remover = StopWordsRemover(inputCol="review_token", outputCol="clean_review")
    cleandf = remover.transform(dftokenized)

    # Reegex option
    #tokenizer = RegexTokenizer(minTokenLength=2,
    #                            inputCol="review_str",
    #                            outputCol="words",
    #                            pattern="\W|[1-9_]")
    #df = tokenizer.transform(df)

    # Positivy logic
    pos_col = array_contains(cleandf["words"], "good")
    df = df.withColumn("positivity", when(pos_col == "true", 1).otherwise(0))
    
    df.select("cid", "positivity").write.mode("overwrite")\
      .parquet("s3://staging-layer2021xxxxx/classification_result.parquet")
    spark.stop()