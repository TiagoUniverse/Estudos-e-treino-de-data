
# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName('Spark Playground').getOrCreate()



df = spark.read.csv("../../../mnt/c/Users/humberto/Downloads/simple-zipcodes.csv", header=True)


# Define a função Python
def processa_local(country, state, city):
    if country == "US":
        return f"Local: {state}"
    elif country == "BR":
        return f"Cidade: {city}"
    else:
        return "Outro país"

# Registra como UDF
processa_local_udf = udf(processa_local, StringType())

# Aplica no DataFrame
df = df.withColumn("descricao_local", processa_local_udf(col("Country"), col("State"), col("City")))
df.show()
