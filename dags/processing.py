from pyspark.sql import SparkSession



def precossing_fct(search_query):
    print(search_query)

def test_spark():
    # Créer une session Spark
    spark = SparkSession.builder \
        .appName("Word Count") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    content = "abcda"

    counts = content.flatMap(lambda line: line.value.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    print(counts)

    spark.stop()