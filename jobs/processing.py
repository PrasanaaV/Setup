from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Word Count") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("hello")

spark.stop()