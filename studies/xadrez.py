from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("my spark") \
    .getOrCreate()



df = spark.read.csv("../data/dados.csv", header=True)


todos_nomes = df.select(col("jogador").alias("nome")).union(df.select(col("adversario").alias("nome")))

todos_nomes.groupBy("nome").count().show()