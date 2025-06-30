from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("my spark") \
    .getOrCreate()

