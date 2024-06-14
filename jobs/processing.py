from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, when
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, array_contains
import json

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Word Count") \
    .master("spark://spark-master:7077") \
    .config("spark.es.nodes", "676d19be3cd5480d980f3eb5dca1dede.us-central1.gcp.cloud.es.io") \
    .config("spark.es.port", "9243") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Lecture du fichier JSON pour le mapping
with open('../files/mapping.json', 'r') as f:
    line_mapping = json.load(f)

# Création d'un DataFrame pour le mapping
mapping_df = spark.createDataFrame(
    [(k, v) for k, v in line_mapping.items()],
    ["original", "mapped"]
)

# Lecture des index Elasticsearch
metro_station_df = spark.read.format("es").load("metro_station")
perimetre_df = spark.read.format("es").load("perimetre")
emplacement_stations_idf_df = spark.read.format("es").load("emplacement-stations-idf")

# Normalisation des noms de station et lignes de métro
metro_station_df = metro_station_df.withColumn("nom", lower(col("nom")))
metro_station_df = metro_station_df.withColumn("nom", regexp_replace(col("nom"), "-", " "))
metro_station_df = metro_station_df.withColumn("nom_split", split(col("nom"), " "))
metro_station_df = metro_station_df.withColumn("number", regexp_replace(col("number"), "ligne", ""))

perimetre_df = perimetre_df.withColumn("ns3_stopname", lower(col("ns3_stopname")))
perimetre_df = perimetre_df.withColumn("ns3_stopname", regexp_replace(col("ns3_stopname"), "-", " "))
perimetre_df = perimetre_df.withColumn("ns3_stopname_split", split(col("ns3_stopname"), " "))
perimetre_df = perimetre_df.withColumn("line", regexp_replace(col("line"), "STIF:Line::C", ""))

emplacement_stations_idf_df = emplacement_stations_idf_df.withColumn("nom_gares", lower(col("nom_gares")))
emplacement_stations_idf_df = emplacement_stations_idf_df.withColumn("nom_gares", regexp_replace(col("nom_gares"), "-", " "))
emplacement_stations_idf_df = emplacement_stations_idf_df.withColumn("nom_gares_split", split(col("nom_gares"), " "))
emplacement_stations_idf_df = emplacement_stations_idf_df.withColumn("res_com", regexp_replace(col("res_com"), "METRO ", ""))
emplacement_stations_idf_df = emplacement_stations_idf_df.withColumn("res_com", regexp_replace(col("res_com"), "RER ", ""))

# Mapping des lignes de métro dans perimetre
perimetre_df = perimetre_df.join(mapping_df, perimetre_df.line == mapping_df.mapped, "left").drop("mapped").withColumnRenamed("original", "line_mapped")
metro_station_df = metro_station_df.join(mapping_df, metro_station_df.number == mapping_df.mapped, "left").drop("mapped").withColumnRenamed("original", "number_mapped")
emplacement_stations_idf_df = emplacement_stations_idf_df.join(mapping_df, emplacement_stations_idf_df.res_com == mapping_df.mapped, "left").drop("mapped").withColumnRenamed("original", "res_com_mapped")

# Fonction pour vérifier si tous les éléments de la plus petite liste sont contenus dans la plus grande liste
def list_contains_all(smaller, larger):
    return array_contains(larger, smaller)

# Jointure des datasets sur les noms de station et lignes de métro
merged_df = metro_station_df \
    .join(perimetre_df, (list_contains_all(metro_station_df.nom_split, perimetre_df.ns3_stopname_split)) & (metro_station_df.number_mapped == perimetre_df.line_mapped)) \
    .join(emplacement_stations_idf_df, (list_contains_all(metro_station_df.nom_split, emplacement_stations_idf_df.nom_gares_split)) & (metro_station_df.number_mapped == emplacement_stations_idf_df.res_com_mapped))

# Sélection des colonnes nécessaires
final_df = merged_df.select(
    metro_station_df["*"],
    perimetre_df.drop("name_line").drop("nom_split").drop("line").columns,
    emplacement_stations_idf_df["longitude"],
    emplacement_stations_idf_df["latitude"]
)

# Enregistrement dans Elasticsearch
final_df.write.format("es").save("clean_perimetre")

spark.stop()
