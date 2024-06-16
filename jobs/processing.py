from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import json

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Word Count") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# API endpoint and keys for each index
elasticsearch_endpoint = "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443"
api_key_metro_station = "TWxtbTdZOEItODFDdXJHT2ptcXI6VkdHWl9kcF9UZmk3ZS05dldMVVV4Zw=="
api_key_perimeter = "QzFsdkc1QUItODFDdXJHT1FuR2g6ZDNxbWdHVXVSQmFzZlZyYW5qSEw4UQ=="
api_key_emplacement_stations_idf = "bkZWbjU0OEJKR3c1U0ExQWJMQXc6RVRESFRDOC1UMTJNMm5lc19CMXFnQQ=="
api_key_clean_index = "NDFrVElaQUItODFDdXJHT1duSFg6LWRfQ2JxeGVROHFvTGNWM0R1ZFM0UQ=="

print("The process begins")

es = Elasticsearch(
        elasticsearch_endpoint,
        api_key=api_key_perimeter
    )

query = {
    "query": {
        "match_all": {}
    },
    "size": 1000
}
res = es.search(index="perimeter", body=query)
data = [hit['_source'] for hit in res['hits']['hits']]
df_pd = pd.DataFrame(data)
perimeter_df = spark.createDataFrame(df_pd)

def elasticsearch_query(es_client, index, query, field):
    body = {
        "query": {
            "simple_query_string": {
                "query": query,
                "fields": [field]
            }
        }
    }
    results = scan(es_client, index=index, query=body)
    return [hit["_source"] for hit in results]

metro_station_es = Elasticsearch(
    elasticsearch_endpoint,
    api_key=api_key_metro_station
)

emplacement_stations_idf_es = Elasticsearch(
    elasticsearch_endpoint,
    api_key=api_key_emplacement_stations_idf
)

# Extraction des noms d'arrêts de perimeter_df
stopnames_list = perimeter_df.select('ns3_stopname').distinct().rdd.flatMap(lambda x: x).collect()

print("The process may take several minutes")
print("step 0/2")

# Batch process Elasticsearch queries
metro_station_results = {}
for element in stopnames_list:
    try:
        metro_station_results[element] = elasticsearch_query(metro_station_es, "metro_station", element, "nom")[0]
    except Exception as e:
        pass

# Create Spark DataFrame
columns = ["ns3_stopname", "maps"] + [str(i) for i in range(1, 10)]
metro_station_updates = [
    (k, v.get("maps"), *[v.get(str(i)) for i in range(1, 10)]) 
    for k, v in metro_station_results.items()
]
metro_station_df = spark.createDataFrame(metro_station_updates, columns)

print("step 1/2")

# Batch process Elasticsearch queries for the second table
emplacement_stations_idf_results = {}
for element in stopnames_list:
    try:
        result = elasticsearch_query(emplacement_stations_idf_es, "emplacement-stations-idf", element, "nom_gares")[0]
        emplacement_stations_idf_results[element] = result
    except Exception as e:
        pass

# Create Spark DataFrame
columns = ["ns3_stopname", "longitude", "latitude"]
emplacement_stations_idf_updates = [
    (k, v.get("longitude"), v.get("latitude"))
    for k, v in emplacement_stations_idf_results.items()
]

emplacement_stations_idf_df = spark.createDataFrame(emplacement_stations_idf_updates, columns)

print("step 2/2")

# Join the updates with the original DataFrame
perimeter_df = perimeter_df.join(metro_station_df, on="ns3_stopname", how="left") \
                           .join(emplacement_stations_idf_df, on="ns3_stopname", how="left")


clean_index_es = Elasticsearch(
    elasticsearch_endpoint,
    api_key=api_key_clean_index
)

# Fonction pour exclure les champs avec des valeurs nulles d'une ligne et ajouter des champs supplémentaires
def exclude_nulls_and_add_fields(row):
    clean_row = {k: v for k, v in row.asDict().items() if v is not None}
    clean_row['destination'] = None
    clean_row['waiting_time'] = None
    return clean_row

# Appliquer la fonction pour exclure les valeurs nulles de chaque ligne et ajouter les nouveaux champs
rdd = perimeter_df.rdd.map(exclude_nulls_and_add_fields)

print("Writing to clean_index in elasticsearch")
print("This may take a few minutes")
# Convertir chaque ligne nettoyée en JSON et envoyer à Elasticsearch
for clean_document in rdd.collect():
    clean_index_es.index(index="clean_index", body=json.dumps(clean_document))

print("End of the process")

spark.stop()