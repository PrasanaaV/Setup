from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, when
import json
from pyspark.sql.functions import col, lower, regexp_replace, split, array_contains

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Word Count") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print('hello')

spark.stop()